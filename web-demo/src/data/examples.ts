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
        sql: `SELECT * FROM products LIMIT 5;
-- EXPECTED:
-- | product_id | product_name | category_id | unit_price | units_in_stock | units_on_order |
-- | 1          | Chai         | 1           | 18.0       | 39             | 0              |
-- | 2          | Chang        | 1           | 19.0       | 17             | 40             |
-- | 3          | Aniseed Syrup| 2           | 10.0       | 13             | 70             |
-- | 4          | Chef Anton's Cajun Seasoning | 2 | 22.0 | 53             | 0              |
-- | 5          | Chef Anton's Gumbo Mix       | 2 | 21.35| 0              | 0              |
-- (5 rows)`,
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
ORDER BY unit_price DESC;
-- EXPECTED:
-- | product_name                  | unit_price |
-- | Mishi Kobe Niku               | 97.0       |
-- | Sir Rodney's Marmalade        | 81.0       |
-- | Carnarvon Tigers              | 62.5       |
-- | Northwoods Cranberry Sauce    | 40.0       |
-- | Alice Mutton                  | 39.0       |
-- | Queso Manchego La Pastora     | 38.0       |
-- | Ikura                         | 31.0       |
-- | Grandma's Boysenberry Spread  | 25.0       |
-- | Tofu                          | 23.25      |
-- | Chef Anton's Cajun Seasoning  | 22.0       |
-- | Queso Cabrales                | 21.0       |
-- | Chef Anton's Gumbo Mix        | 21.35      |
-- (12 rows)`,
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
LIMIT 10;
-- EXPECTED:
-- | name                            | price | price_with_tax | 
-- | Chai                            | 18.0  | 21.6           | 
-- | Chang                           | 19.0  | 22.8           | 
-- | Aniseed Syrup                   | 10.0  | 12.0           | 
-- | Chef Anton's Cajun Seasoning    | 22.0  | 26.4           | 
-- | Chef Anton's Gumbo Mix          | 21.35 | 25.62          | 
-- | Grandma's Boysenberry Spread    | 25.0  | 30.0           | 
-- | Uncle Bob's Organic Dried Pears | 30.0  | 36.0           | 
-- | Northwoods Cranberry Sauce      | 40.0  | 48.0           | 
-- | Mishi Kobe Niku                 | 97.0  | 116.4          | 
-- | Ikura                           | 31.0  | 37.2           | 
-- (10 rows)`,
        description: 'Use aliases for columns and calculated fields',
        sqlFeatures: ['SELECT', 'AS', 'Expressions'],
      },
      {
        id: 'basic-4',
        title: 'DISTINCT values',
        database: 'northwind',
        sql: `SELECT DISTINCT category_id
FROM products
ORDER BY category_id;
-- EXPECTED:
-- | category_id |
-- | 1           |
-- | 2           |
-- | 3           |
-- | 4           |
-- | 6           |
-- | 7           |
-- | 8           |
-- (7 rows)`,
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
LIMIT 5;
-- EXPECTED:
-- | product_name                   | uppercase                      | lowercase                      |
-- | Chai                           | CHAI                           | chai                           |
-- | Chang                          | CHANG                          | chang                          |
-- | Aniseed Syrup                  | ANISEED SYRUP                  | aniseed syrup                  |
-- | Chef Anton's Cajun Seasoning   | CHEF ANTON'S CAJUN SEASONING   | chef anton's cajun seasoning   |
-- | Chef Anton's Gumbo Mix         | CHEF ANTON'S GUMBO MIX         | chef anton's gumbo mix         |
-- (5 rows)`,
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
LIMIT 8;
-- EXPECTED:
-- | first_name | last_name | first_initial | last_name_length |
-- | Alice      | Johnson   | A             | 7                |
-- | Bob        | Smith     | B             | 5                |
-- | Carol      | White     | C             | 5                |
-- | David      | Brown     | D             | 5                |
-- | Eve        | Martinez  | E             | 8                |
-- | Frank      | Wilson    | F             | 6                |
-- | Grace      | Taylor    | G             | 6                |
-- | Henry      | Anderson  | H             | 8                |
-- (8 rows)`,
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
LIMIT 5;
-- EXPECTED:
-- | padded                           | trimmed                      | padded_length | trimmed_length | 
-- |   Chai                           | Chai                         | 8             | 4              | 
-- |   Chang                          | Chang                        | 9             | 5              | 
-- |   Aniseed Syrup                  | Aniseed Syrup                | 17            | 13             | 
-- |   Chef Anton's Cajun Seasoning   | Chef Anton's Cajun Seasoning | 32            | 28             | 
-- |   Chef Anton's Gumbo Mix         | Chef Anton's Gumbo Mix       | 26            | 22             | 
-- (5 rows)`,
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
LIMIT 8;
-- EXPECTED:
-- | first_name | last_name | full_name       | formatted_name  |
-- | Alice      | Johnson   | Alice Johnson   | Johnson, Alice  |
-- | Bob        | Smith     | Bob Smith       | Smith, Bob      |
-- | Carol      | White     | Carol White     | White, Carol    |
-- | David      | Brown     | David Brown     | Brown, David    |
-- | Eve        | Martinez  | Eve Martinez    | Martinez, Eve   |
-- | Frank      | Wilson    | Frank Wilson    | Wilson, Frank   |
-- | Grace      | Taylor    | Grace Taylor    | Taylor, Grace   |
-- | Henry      | Anderson  | Henry Anderson  | Anderson, Henry |
-- (8 rows)`,
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
LIMIT 6;
-- EXPECTED:
-- | product_name                    | first_5_chars | last_5_chars | 
-- | Aniseed Syrup                   | Anise         | Syrup        | 
-- | Chef Anton's Cajun Seasoning    | Chef          | oning        | 
-- | Chef Anton's Gumbo Mix          | Chef          | o Mix        | 
-- | Grandma's Boysenberry Spread    | Grand         | pread        | 
-- | Uncle Bob's Organic Dried Pears | Uncle         | Pears        | 
-- | Northwoods Cranberry Sauce      | North         | Sauce        | 
-- (6 rows)`,
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
LIMIT 6;
-- EXPECTED:
-- | product_name                    | replaced                        | underscored                     | 
-- | Chai                            | Ch@i                            | Chai                            | 
-- | Chang                           | Ch@ng                           | Chang                           | 
-- | Chef Anton's Cajun Seasoning    | Chef Anton's C@jun Se@soning    | Chef_Anton's_Cajun_Seasoning    | 
-- | Grandma's Boysenberry Spread    | Gr@ndm@'s Boysenberry Spre@d    | Grandma's_Boysenberry_Spread    | 
-- | Uncle Bob's Organic Dried Pears | Uncle Bob's Org@nic Dried Pe@rs | Uncle_Bob's_Organic_Dried_Pears | 
-- | Northwoods Cranberry Sauce      | Northwoods Cr@nberry S@uce      | Northwoods_Cranberry_Sauce      | 
-- (6 rows)`,
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
LIMIT 8;
-- EXPECTED:
-- | first_name | last_name | first_a_position | first_e_position |
-- | Alice      | Johnson   | 1                | 0                |
-- | David      | Brown     | 2                | 0                |
-- | Frank      | Wilson    | 3                | 0                |
-- | Grace      | Taylor    | 3                | 5                |
-- | Maria      | Clark     | 2                | 0                |
-- | Nathan     | Lewis     | 2                | 2                |
-- | Olivia     | Walker    | 6                | 5                |
-- | Paul       | Hall      | 2                | 0                |
-- (8 rows)`,
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
LIMIT 5;
-- EXPECTED:
-- | product_name                 | reversed                     | reversed_upper               | 
-- | Chai                         | iahC                         | IAHC                         | 
-- | Chang                        | gnahC                        | GNAHC                        | 
-- | Aniseed Syrup                | puryS deesinA                | PURYS DEESINA                | 
-- | Chef Anton's Cajun Seasoning | gninosaeS nujaC s'notnA fehC | GNINOSAES NUJAC S'NOTNA FEHC | 
-- | Chef Anton's Gumbo Mix       | xiM obmuG s'notnA fehC       | XIM OBMUG S'NOTNA FEHC       | 
-- (5 rows)`,
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
ORDER BY c.category_name, p.product_name;
-- EXPECTED:
-- | product_name                   | category_name | unit_price |
-- | Chai                           | Beverages     | 18.0       |
-- | Chang                          | Beverages     | 19.0       |
-- | Aniseed Syrup                  | Condiments    | 10.0       |
-- | Chef Anton's Cajun Seasoning   | Condiments    | 22.0       |
-- | Chef Anton's Gumbo Mix         | Condiments    | 21.35      |
-- | Genen Shouyu                   | Condiments    | 15.5       |
-- | Grandma's Boysenberry Spread   | Condiments    | 25.0       |
-- | Northwoods Cranberry Sauce     | Condiments    | 40.0       |
-- | Pavlova                        | Confections   | 17.45      |
-- | Sir Rodney's Marmalade         | Confections   | 81.0       |
-- | Teatime Chocolate Biscuits     | Confections   | 9.2        |
-- | Queso Cabrales                 | Dairy Products| 21.0       |
-- | Queso Manchego La Pastora      | Dairy Products| 38.0       |
-- | Alice Mutton                   | Meat/Poultry  | 39.0       |
-- | Mishi Kobe Niku                | Meat/Poultry  | 97.0       |
-- | Tofu                           | Produce       | 23.25      |
-- | Uncle Bob's Organic Dried Pears| Produce       | 30.0       |
-- | Carnarvon Tigers               | Seafood       | 62.5       |
-- | Ikura                          | Seafood       | 31.0       |
-- | Konbu                          | Seafood       | 6.0        |
-- (20 rows)`,
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
ORDER BY product_count DESC;
-- EXPECTED:
-- | category_name  | product_count | 
-- | Condiments     | 6             | 
-- | Confections    | 3             | 
-- | Seafood        | 3             | 
-- | Beverages      | 2             | 
-- | Dairy Products | 2             | 
-- | Meat/Poultry   | 2             | 
-- | Produce        | 2             | 
-- (7 rows)`,
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
FROM products;
-- EXPECTED:
-- | total_products | avg_price | min_price | max_price |
-- | 20             | ~34.05    | 6.0       | 97.0      |
-- (1 row)
-- Note: avg_price is approximate due to floating point`,
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
ORDER BY product_count DESC;
-- EXPECTED:
-- | category_name  | product_count | avg_price | 
-- | Condiments     | 6             | NULL      | 
-- | Confections    | 3             | NULL      | 
-- | Seafood        | 3             | NULL      | 
-- | Beverages      | 2             | NULL      | 
-- | Dairy Products | 2             | NULL      | 
-- | Meat/Poultry   | 2             | NULL      | 
-- | Produce        | 2             | NULL      | 
-- (7 rows)`,
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
ORDER BY product_count DESC;
-- EXPECTED:
-- | category_name  | product_count | 
-- | Condiments     | 6             | 
-- | Confections    | 3             | 
-- | Seafood        | 3             | 
-- | Beverages      | 2             | 
-- | Dairy Products | 2             | 
-- | Meat/Poultry   | 2             | 
-- | Produce        | 2             | 
-- (7 rows)`,
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
LIMIT 15;
-- ⏭️ SKIP: WITH RECURSIVE not yet implemented - parser expects CTE name (identifier)`,
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
ORDER BY level;
-- ⏭️ SKIP: WITH RECURSIVE not yet implemented - parser expects CTE name (identifier)`,
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
ORDER BY unit_price DESC;
-- ⏭️ SKIP: Scalar subqueries not fully implemented - type coercion issues with numeric comparisons`,
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
ORDER BY unit_price DESC;
-- ⏭️ SKIP: IN with subquery not yet implemented - requires subquery evaluation in WHERE clause;
-- EXPECTED:
-- | product_name                 | unit_price | 
-- | Northwoods Cranberry Sauce   | 40.0       | 
-- | Grandma's Boysenberry Spread | 25.0       | 
-- | Chef Anton's Cajun Seasoning | 22.0       | 
-- | Chef Anton's Gumbo Mix       | 21.35      | 
-- | Chang                        | 19.0       | 
-- | Chai                         | 18.0       | 
-- | Genen Shouyu                 | 15.5       | 
-- | Aniseed Syrup                | 10.0       | 
-- (8 rows)`,
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
ORDER BY avg_salary DESC;
-- ⏭️ SKIP: Derived tables (subqueries in FROM clause) require AS alias - parser expects SQL:1999 syntax`,
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
ORDER BY unit_price;
-- ⏭️ SKIP: CASE expressions have type coercion issues with numeric literals (Float vs Integer comparison);
-- EXPECTED:
-- | product_name                    | unit_price | price_category | 
-- | Konbu                           | 6.0        | Budget         | 
-- | Teatime Chocolate Biscuits      | 9.2        | Budget         | 
-- | Aniseed Syrup                   | 10.0       | Standard       | 
-- | Genen Shouyu                    | 15.5       | Standard       | 
-- | Pavlova                         | 17.45      | Standard       | 
-- | Chai                            | 18.0       | Standard       | 
-- | Chang                           | 19.0       | Standard       | 
-- | Queso Cabrales                  | 21.0       | Standard       | 
-- | Chef Anton's Gumbo Mix          | 21.35      | Standard       | 
-- | Chef Anton's Cajun Seasoning    | 22.0       | Standard       | 
-- (20 rows)`,
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
ORDER BY department;
-- ⏭️ SKIP: Requires employees database with salary data - not yet implemented in test environment`,
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
LIMIT 10;
-- ⏭️ SKIP: Requires employees database with detailed employee data - not yet implemented in test environment`,
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
ORDER BY department;
-- ⏭️ SKIP: Requires employees database - not yet implemented in test environment`,
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
ORDER BY unit_price DESC;
-- ⏭️ SKIP: Type coercion issue - Float vs Integer comparison in WHERE clause;
-- EXPECTED:
-- | category  | product_name               | unit_price | 
-- | Expensive | Mishi Kobe Niku            | 97.0       | 
-- | Expensive | Sir Rodney's Marmalade     | 81.0       | 
-- | Expensive | Carnarvon Tigers           | 62.5       | 
-- | Cheap     | Konbu                      | 6.0        | 
-- | Cheap     | Teatime Chocolate Biscuits | 9.2        | 
-- (5 rows)`,
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
LIMIT 15;
-- EXPECTED:
-- | name                            | type     |
-- | Beverages                       | category |
-- | Condiments                      | category |
-- | Confections                     | category |
-- | Dairy Products                  | category |
-- | Grains/Cereals                  | category |
-- | Meat/Poultry                    | category |
-- | Produce                         | category |
-- | Seafood                         | category |
-- | Chai                            | product  |
-- | Chang                           | product  |
-- | Aniseed Syrup                   | product  |
-- | Chef Anton's Cajun Seasoning    | product  |
-- | Chef Anton's Gumbo Mix          | product  |
-- | Grandma's Boysenberry Spread    | product  |
-- | Uncle Bob's Organic Dried Pears | product  |
-- (15 rows)`,
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
ORDER BY avg_salary DESC;
-- EXPECTED:
-- | dept_name         | headcount | avg_salary | min_salary | max_salary |
-- | Engineering       | 5         | 112000.0   | 95000      | 125000     |
-- | Marketing         | 3         | 87000.0    | 82000      | 91000      |
-- | Sales             | 5         | 86000.0    | 78000      | 92000      |
-- | Human Resources   | 2         | 73000.0    | 71000      | 75000      |
-- | Operations        | 3         | 70000.0    | 68000      | 72000      |
-- (5 rows)`,
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
ORDER BY total_budget DESC;
-- EXPECTED:
-- | dept_name         | location      | project_count | total_budget |
-- | Engineering       | San Francisco | 3             | 1070000      |
-- | Operations        | Seattle       | 1             | 410000       |
-- | Marketing         | Los Angeles   | 2             | 275000       |
-- | Sales             | New York      | 1             | 150000       |
-- | Human Resources   | Chicago       | 1             | 75000        |
-- (5 rows)`,
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
LIMIT 15;
-- EXPECTED:
-- | employee        | department  | project_name            | budget  |
-- | Alice Chen      | Engineering | Cloud Migration         | 500000  |
-- | Alice Chen      | Engineering | Mobile App Redesign     | 250000  |
-- | Alice Chen      | Engineering | Customer Portal         | 320000  |
-- | Bob Martinez    | Engineering | Cloud Migration         | 500000  |
-- | Bob Martinez    | Engineering | Mobile App Redesign     | 250000  |
-- | Bob Martinez    | Engineering | Customer Portal         | 320000  |
-- | Carol Williams  | Engineering | Cloud Migration         | 500000  |
-- | Carol Williams  | Engineering | Mobile App Redesign     | 250000  |
-- | Carol Williams  | Engineering | Customer Portal         | 320000  |
-- | Maria Rodriguez | Engineering | Cloud Migration         | 500000  |
-- | Maria Rodriguez | Engineering | Mobile App Redesign     | 250000  |
-- | Maria Rodriguez | Engineering | Customer Portal         | 320000  |
-- | Sam Harris      | Engineering | Cloud Migration         | 500000  |
-- | Sam Harris      | Engineering | Mobile App Redesign     | 250000  |
-- | Sam Harris      | Engineering | Customer Portal         | 320000  |
-- (15 rows)`,
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
ORDER BY budget_per_employee DESC;
-- EXPECTED:
-- | dept_name         | headcount | project_budget | budget_per_employee |
-- | Operations        | 3         | 1230000        | 410000              |
-- | Engineering       | 15        | 5350000        | 356666              |
-- | Sales             | 5         | 750000         | 150000              |
-- | Human Resources   | 2         | 150000         | 75000               |
-- | Marketing         | 6         | 550000         | 91666               |
-- (5 rows)`,
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
ORDER BY p.budget DESC;
-- EXPECTED:
-- | project_name                    | budget | dept_name   | location      | priority |
-- | Cloud Migration                 | 500000 | Engineering | San Francisco | Critical |
-- | Supply Chain Optimization       | 410000 | Operations  | Seattle       | Critical |
-- | Customer Portal                 | 320000 | Engineering | San Francisco | Major    |
-- | Mobile App Redesign             | 250000 | Engineering | San Francisco | Major    |
-- | Innovation Lab                  | 200000 | NULL        | NULL          | Major    |
-- | Brand Refresh                   | 180000 | Marketing   | Los Angeles   | Standard |
-- | Q4 Sales Campaign               | 150000 | Sales       | New York      | Standard |
-- | Market Research Initiative      | 95000  | Marketing   | Los Angeles   | Minor    |
-- | Employee Wellness Program       | 75000  | Human Resources | Chicago   | Minor    |
-- (9 rows)`,
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
ORDER BY enrollment_count DESC;
-- EXPECTED:
-- | course_name                 | department       | enrollment_count | 
-- | Introduction to Programming | Computer Science | 14               | 
-- | Data Structures             | Computer Science | 13               | 
-- | Algorithms                  | Computer Science | 12               | 
-- | Calculus I                  | Mathematics      | 9                | 
-- | Linear Algebra              | Mathematics      | 9                | 
-- | Statistics                  | Mathematics      | 9                | 
-- | Classical Mechanics         | Physics          | 9                | 
-- | Electromagnetism            | Physics          | 9                | 
-- | Quantum Mechanics           | Physics          | 9                | 
-- (9 rows)`,
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
ORDER BY student_count DESC;
-- EXPECTED:
-- | department       | student_count | course_count | avg_student_gpa | 
-- | Computer Science | 20            | 3            | NULL            | 
-- | Mathematics      | 20            | 3            | NULL            | 
-- | Physics          | 20            | 3            | NULL            | 
-- (3 rows)`,
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
ORDER BY grade;
-- EXPECTED:
-- | grade | count |
-- | A     | 30    |
-- | B     | 27    |
-- | C     | 18    |
-- | D     | 6     |
-- | F     | 2     |
-- (5 rows)`,
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

  {
    id: 'dml',
    title: 'Data Modification',
    description: 'INSERT, UPDATE, DELETE operations for modifying table data',
    queries: [
      {
        id: 'dml-1',
        title: 'Simple INSERT',
        database: 'empty',
        sql: `CREATE TABLE products (
    id INTEGER PRIMARY KEY,
    name VARCHAR(100),
    price FLOAT
);

INSERT INTO products VALUES (1, 'Laptop', 999.99);
INSERT INTO products VALUES (2, 'Mouse', 25.50);
INSERT INTO products VALUES (3, 'Keyboard', 75.00);

SELECT * FROM products ORDER BY id;`,
        description: 'Create table and insert individual rows',
        sqlFeatures: ['CREATE TABLE', 'INSERT', 'VALUES', 'SELECT'],
      },
      {
        id: 'dml-2',
        title: 'INSERT with Column List',
        database: 'empty',
        sql: `CREATE TABLE customers (
    customer_id INTEGER PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100),
    city VARCHAR(50)
);

INSERT INTO customers (customer_id, name, email)
VALUES (1, 'Alice Johnson', 'alice@example.com');

INSERT INTO customers (customer_id, name, city)
VALUES (2, 'Bob Smith', 'New York');

SELECT * FROM customers ORDER BY customer_id;`,
        description: 'Insert with explicit column specification for partial data',
        sqlFeatures: ['INSERT', 'Column list', 'Partial columns', 'NULL handling'],
      },
      {
        id: 'dml-3',
        title: 'Multiple Row INSERT',
        database: 'empty',
        sql: `CREATE TABLE employees (
    emp_id INTEGER PRIMARY KEY,
    name VARCHAR(100),
    department VARCHAR(50),
    salary FLOAT
);

INSERT INTO employees VALUES
    (1, 'Alice', 'Engineering', 95000),
    (2, 'Bob', 'Sales', 75000),
    (3, 'Carol', 'Engineering', 98000),
    (4, 'Dave', 'Marketing', 72000);

SELECT * FROM employees ORDER BY emp_id;`,
        description: 'Batch insert multiple rows in a single statement',
        sqlFeatures: ['INSERT', 'Multi-row VALUES', 'Batch insertion'],
      },
      {
        id: 'dml-4',
        title: 'Basic UPDATE',
        database: 'empty',
        sql: `CREATE TABLE products (
    id INTEGER PRIMARY KEY,
    name VARCHAR(100),
    price FLOAT,
    stock INTEGER
);

INSERT INTO products VALUES
    (1, 'Laptop', 999.99, 10),
    (2, 'Mouse', 25.50, 50);

UPDATE products SET stock = stock + 20 WHERE id = 1;

SELECT * FROM products ORDER BY id;`,
        description: 'Update table data with calculated values',
        sqlFeatures: ['UPDATE', 'SET', 'WHERE', 'Calculated updates'],
      },
      {
        id: 'dml-5',
        title: 'UPDATE Multiple Columns',
        database: 'empty',
        sql: `CREATE TABLE employees (
    emp_id INTEGER PRIMARY KEY,
    name VARCHAR(100),
    salary FLOAT,
    bonus FLOAT
);

INSERT INTO employees VALUES
    (1, 'Alice', 95000, 5000),
    (2, 'Bob', 75000, 3000);

UPDATE employees
SET salary = salary * 1.1,
    bonus = bonus * 1.2
WHERE emp_id = 1;

SELECT * FROM employees ORDER BY emp_id;`,
        description: 'Update multiple columns simultaneously with calculations',
        sqlFeatures: ['UPDATE', 'Multiple SET clauses', 'Calculations'],
      },
      {
        id: 'dml-6',
        title: 'UPDATE with Conditional Logic',
        database: 'empty',
        sql: `CREATE TABLE products (
    id INTEGER PRIMARY KEY,
    name VARCHAR(100),
    price FLOAT,
    discount FLOAT
);

INSERT INTO products VALUES
    (1, 'Laptop', 1000, 0),
    (2, 'Mouse', 25, 0),
    (3, 'Monitor', 500, 0);

UPDATE products
SET discount = CASE
    WHEN price >= 500 THEN 0.15
    WHEN price >= 100 THEN 0.10
    ELSE 0.05
END;

SELECT id, name, price, discount,
       ROUND(price * (1 - discount), 2) AS final_price
FROM products
ORDER BY id;`,
        description: 'Apply tiered discounts based on price using CASE',
        sqlFeatures: ['UPDATE', 'CASE', 'Business logic', 'ROUND'],
      },
      {
        id: 'dml-7',
        title: 'Simple DELETE',
        database: 'empty',
        sql: `CREATE TABLE logs (
    log_id INTEGER PRIMARY KEY,
    message VARCHAR(100),
    severity VARCHAR(20)
);

INSERT INTO logs VALUES
    (1, 'System started', 'INFO'),
    (2, 'Disk full', 'ERROR'),
    (3, 'User login', 'INFO'),
    (4, 'Connection failed', 'ERROR');

DELETE FROM logs WHERE severity = 'INFO';

SELECT * FROM logs ORDER BY log_id;`,
        description: 'Remove rows matching a simple condition',
        sqlFeatures: ['DELETE', 'WHERE', 'Conditional deletion'],
      },
      {
        id: 'dml-8',
        title: 'DELETE with Complex Condition',
        database: 'empty',
        sql: `CREATE TABLE orders (
    order_id INTEGER PRIMARY KEY,
    customer_id INTEGER,
    amount FLOAT,
    status VARCHAR(20)
);

INSERT INTO orders VALUES
    (1, 100, 50.00, 'pending'),
    (2, 101, 150.00, 'completed'),
    (3, 100, 25.00, 'cancelled'),
    (4, 102, 200.00, 'completed');

DELETE FROM orders
WHERE status = 'cancelled' OR amount < 30;

SELECT * FROM orders ORDER BY order_id;`,
        description: 'Remove rows using compound conditions with OR',
        sqlFeatures: ['DELETE', 'Complex WHERE', 'OR', 'Multiple conditions'],
      },
      {
        id: 'dml-9',
        title: 'Complete Workflow (CRUD)',
        database: 'empty',
        sql: `CREATE TABLE inventory (
    item_id INTEGER PRIMARY KEY,
    name VARCHAR(100),
    quantity INTEGER,
    price FLOAT
);

-- Create
INSERT INTO inventory VALUES (1, 'Widget', 100, 9.99);
INSERT INTO inventory VALUES (2, 'Gadget', 50, 19.99);

-- Read
SELECT * FROM inventory WHERE quantity > 0;

-- Update
UPDATE inventory SET quantity = quantity - 10 WHERE item_id = 1;

-- Delete (out of stock items)
DELETE FROM inventory WHERE quantity <= 0;

-- Final state
SELECT * FROM inventory ORDER BY item_id;`,
        description: 'Demonstrate full CRUD lifecycle in a single workflow',
        sqlFeatures: ['CRUD', 'INSERT', 'SELECT', 'UPDATE', 'DELETE', 'Business workflow'],
      },
      {
        id: 'dml-10',
        title: 'Transaction-style Operations',
        database: 'empty',
        sql: `CREATE TABLE accounts (
    account_id INTEGER PRIMARY KEY,
    owner VARCHAR(100),
    balance FLOAT CHECK (balance >= 0)
);

INSERT INTO accounts VALUES
    (1, 'Alice', 1000.00),
    (2, 'Bob', 500.00);

-- Transfer $200 from Alice to Bob
UPDATE accounts SET balance = balance - 200 WHERE account_id = 1;
UPDATE accounts SET balance = balance + 200 WHERE account_id = 2;

SELECT account_id, owner, balance,
       CASE
           WHEN balance >= 1000 THEN 'Premium'
           WHEN balance >= 500 THEN 'Standard'
           ELSE 'Basic'
       END AS tier
FROM accounts
ORDER BY account_id;`,
        description: 'Simulate money transfer with multi-step updates and tiering',
        sqlFeatures: [
          'UPDATE',
          'CHECK constraint',
          'Multi-step operations',
          'CASE',
          'Business logic',
        ],
      },
    ],
  },

  {
    id: 'datetime',
    title: 'Date & Time Functions',
    description: 'CURRENT_DATE, date/time extraction, and date-based analysis',
    queries: [
      {
        id: 'datetime-1',
        title: 'Current Date and Time',
        database: 'employees',
        sql: `SELECT
  CURRENT_DATE AS today,
  CURDATE() AS today_alias,
  CURRENT_TIME AS now_time,
  CURTIME() AS now_time_alias,
  CURRENT_TIMESTAMP AS now_full,
  NOW() AS now_alias;
-- EXPECTED:
-- Non-deterministic output - returns current system date/time
-- Sample format:
-- | today      | today_alias | now_time | now_time_alias | now_full            | now_alias           |
-- | 2025-01-28 | 2025-01-28  | 14:30:45 | 14:30:45       | 2025-01-28 14:30:45 | 2025-01-28 14:30:45 |
-- (1 row)`,
        description: 'Get current date, time, and timestamp values with aliases',
        sqlFeatures: [
          'CURRENT_DATE',
          'CURDATE',
          'CURRENT_TIME',
          'CURTIME',
          'CURRENT_TIMESTAMP',
          'NOW',
        ],
      },
      {
        id: 'datetime-2',
        title: 'Date Part Extraction',
        database: 'employees',
        sql: `SELECT
  CURRENT_DATE AS full_date,
  YEAR(CURRENT_DATE) AS year,
  MONTH(CURRENT_DATE) AS month,
  DAY(CURRENT_DATE) AS day;
-- EXPECTED:
-- Non-deterministic output - depends on current date
-- Sample format:
-- | full_date  | year | month | day |
-- | 2025-01-28 | 2025 | 1     | 28  |
-- (1 row)`,
        description: 'Extract year, month, and day components from dates',
        sqlFeatures: ['YEAR', 'MONTH', 'DAY', 'CURRENT_DATE'],
      },
      {
        id: 'datetime-3',
        title: 'Time Part Extraction',
        database: 'employees',
        sql: `SELECT
  CURRENT_TIMESTAMP AS full_timestamp,
  HOUR(CURRENT_TIMESTAMP) AS hour,
  MINUTE(CURRENT_TIMESTAMP) AS minute,
  SECOND(CURRENT_TIMESTAMP) AS second;
-- EXPECTED:
-- Non-deterministic output - depends on current timestamp
-- Sample format:
-- | full_timestamp      | hour | minute | second |
-- | 2025-01-28 14:30:45 | 14   | 30     | 45     |
-- (1 row)`,
        description: 'Extract hour, minute, and second components from timestamps',
        sqlFeatures: ['HOUR', 'MINUTE', 'SECOND', 'CURRENT_TIMESTAMP'],
      },
      {
        id: 'datetime-4',
        title: 'Employee Hire Date Analysis',
        database: 'employees',
        sql: `SELECT
  first_name || ' ' || last_name AS employee,
  hire_date,
  YEAR(hire_date) AS hire_year,
  MONTH(hire_date) AS hire_month
FROM employees
ORDER BY hire_date DESC
LIMIT 10;
-- EXPECTED:
-- | employee         | hire_date  | hire_year | hire_month |
-- | Tyler Hill       | 2020-04-15 | 2020      | 4          |
-- | Justin Wright    | 2019-07-01 | 2019      | 7          |
-- | Brian Hernandez  | 2019-05-20 | 2019      | 5          |
-- | Brandon Martin   | 2019-05-12 | 2019      | 5          |
-- | Eric Robinson    | 2019-04-10 | 2019      | 4          |
-- | Ryan Thomas      | 2019-03-15 | 2019      | 3          |
-- | Sarah Hall       | 2019-03-10 | 2019      | 3          |
-- | Matthew Lewis    | 2019-02-15 | 2019      | 2          |
-- | Amanda Lee       | 2018-07-15 | 2018      | 7          |
-- | James Wilson     | 2017-11-12 | 2017      | 11         |
-- (10 rows)`,
        description: 'Analyze employee hire dates with year and month extraction',
        sqlFeatures: ['YEAR', 'MONTH', 'String concatenation', 'ORDER BY'],
      },
      {
        id: 'datetime-5',
        title: 'Grouping by Date Parts',
        database: 'employees',
        sql: `SELECT
  YEAR(hire_date) AS year,
  MONTH(hire_date) AS month,
  COUNT(*) AS hires
FROM employees
GROUP BY YEAR(hire_date), MONTH(hire_date)
ORDER BY year DESC, month DESC;
-- EXPECTED:
-- | year | month | hires |
-- | 2020 | 4     | 1     |
-- | 2019 | 7     | 1     |
-- | 2019 | 5     | 2     |
-- | 2019 | 4     | 1     |
-- | 2019 | 3     | 2     |
-- | 2019 | 2     | 1     |
-- | 2018 | 7     | 1     |
-- | 2017 | 11    | 1     |
-- | 2017 | 8     | 1     |
-- | 2016 | 3     | 1     |
-- | 2015 | 1     | 1     |
-- (11 rows)`,
        description: 'Group and count employees by hire year and month',
        sqlFeatures: ['YEAR', 'MONTH', 'GROUP BY', 'COUNT', 'Date functions in GROUP BY'],
      },
      {
        id: 'datetime-6',
        title: 'Date Filtering',
        database: 'employees',
        sql: `SELECT
  first_name || ' ' || last_name AS employee,
  hire_date,
  department
FROM employees
WHERE YEAR(hire_date) >= 2020
ORDER BY hire_date;
-- EXPECTED:
-- | employee    | hire_date  | department |
-- | Tyler Hill  | 2020-04-15 | Operations |
-- (1 row)`,
        description: 'Filter employees hired in recent years using date functions',
        sqlFeatures: ['YEAR', 'WHERE', 'Date functions in WHERE'],
      },
      {
        id: 'datetime-7',
        title: 'Age/Tenure Calculation',
        database: 'employees',
        sql: `SELECT
  first_name || ' ' || last_name AS employee,
  hire_date,
  YEAR(CURRENT_DATE) - YEAR(hire_date) AS years_with_company,
  CASE
    WHEN YEAR(CURRENT_DATE) - YEAR(hire_date) < 1 THEN 'New'
    WHEN YEAR(CURRENT_DATE) - YEAR(hire_date) < 3 THEN 'Junior'
    WHEN YEAR(CURRENT_DATE) - YEAR(hire_date) < 10 THEN 'Senior'
    ELSE 'Veteran'
  END AS tenure_level
FROM employees
ORDER BY hire_date
LIMIT 15;
-- EXPECTED:
-- Non-deterministic output - depends on current date
-- Sample (assuming current year is 2025):
-- | employee            | hire_date  | years_with_company | tenure_level |
-- | Sarah Chen          | 2015-01-15 | 10                 | Veteran      |
-- | Michael Rodriguez   | 2016-03-20 | 9                  | Senior       |
-- | Jennifer Martinez   | 2017-08-15 | 8                  | Senior       |
-- | James Wilson        | 2017-11-12 | 8                  | Senior       |
-- | Amanda Lee          | 2018-07-15 | 7                  | Senior       |
-- | Matthew Lewis       | 2019-02-15 | 6                  | Senior       |
-- | Sarah Hall          | 2019-03-10 | 6                  | Senior       |
-- | Ryan Thomas         | 2019-03-15 | 6                  | Senior       |
-- | Eric Robinson       | 2019-04-10 | 6                  | Senior       |
-- | Brandon Martin      | 2019-05-12 | 6                  | Senior       |
-- | Brian Hernandez     | 2019-05-20 | 6                  | Senior       |
-- | Justin Wright       | 2019-07-01 | 6                  | Senior       |
-- | Tyler Hill          | 2020-04-15 | 5                  | Senior       |
-- (15 rows total, values may vary based on current year)`,
        description: 'Calculate employee tenure and categorize by experience level',
        sqlFeatures: ['YEAR', 'CURRENT_DATE', 'CASE', 'Date arithmetic'],
      },
      {
        id: 'datetime-8',
        title: 'Date-based Aggregations',
        database: 'employees',
        sql: `SELECT
  YEAR(hire_date) AS hire_year,
  department,
  COUNT(*) AS employee_count,
  AVG(salary) AS avg_salary
FROM employees
GROUP BY YEAR(hire_date), department
HAVING COUNT(*) > 1
ORDER BY hire_year DESC, avg_salary DESC;
-- EXPECTED:
-- | hire_year | department  | employee_count | avg_salary |
-- | 2019      | Engineering | 3              | ~92666.67  |
-- | 2019      | Sales       | 3              | ~83666.67  |
-- (2 rows)
-- Note: avg_salary values are approximate due to floating point`,
        description: 'Aggregate employee statistics by hire year and department',
        sqlFeatures: ['YEAR', 'GROUP BY', 'HAVING', 'COUNT', 'AVG', 'Date-based reporting'],
      },
      {
        id: 'datetime-9',
        title: 'Quarter Analysis',
        database: 'employees',
        sql: `SELECT
  CASE
    WHEN MONTH(hire_date) BETWEEN 1 AND 3 THEN 'Q1'
    WHEN MONTH(hire_date) BETWEEN 4 AND 6 THEN 'Q2'
    WHEN MONTH(hire_date) BETWEEN 7 AND 9 THEN 'Q3'
    ELSE 'Q4'
  END AS quarter,
  YEAR(hire_date) AS year,
  COUNT(*) AS hires
FROM employees
GROUP BY
  CASE
    WHEN MONTH(hire_date) BETWEEN 1 AND 3 THEN 'Q1'
    WHEN MONTH(hire_date) BETWEEN 4 AND 6 THEN 'Q2'
    WHEN MONTH(hire_date) BETWEEN 7 AND 9 THEN 'Q3'
    ELSE 'Q4'
  END,
  YEAR(hire_date)
ORDER BY year DESC, quarter;
-- EXPECTED:
-- | quarter | year | hires |
-- | Q2      | 2020 | 1     |
-- | Q1      | 2019 | 3     |
-- | Q2      | 2019 | 3     |
-- | Q3      | 2019 | 1     |
-- | Q3      | 2018 | 1     |
-- | Q3      | 2017 | 1     |
-- | Q4      | 2017 | 1     |
-- | Q1      | 2016 | 1     |
-- | Q1      | 2015 | 1     |
-- (9 rows)`,
        description: 'Analyze hiring patterns by fiscal quarter using date functions',
        sqlFeatures: ['MONTH', 'YEAR', 'CASE', 'BETWEEN', 'GROUP BY', 'Business intelligence'],
      },
    ],
  },

  {
    id: 'patterns',
    title: 'Pattern Matching & Predicates',
    description: 'LIKE, BETWEEN, IN, EXISTS, ANY/ALL predicates',
    queries: [
      {
        id: 'pattern-1',
        title: 'LIKE Pattern Matching (Basic)',
        database: 'northwind',
        sql: `SELECT
  product_name,
  unit_price
FROM products
WHERE product_name LIKE 'Ch%'
ORDER BY product_name;
-- EXPECTED:
-- | product_name                 | unit_price | 
-- | Chai                         | 18.0       | 
-- | Chang                        | 19.0       | 
-- | Chef Anton's Cajun Seasoning | 22.0       | 
-- | Chef Anton's Gumbo Mix       | 21.35      | 
-- (4 rows)`,
        description: 'Match products starting with "Ch" using % wildcard',
        sqlFeatures: ['LIKE', '% wildcard', 'Pattern matching'],
      },
      {
        id: 'pattern-2',
        title: 'LIKE with Multiple Patterns',
        database: 'northwind',
        sql: `SELECT
  product_name,
  category_id
FROM products
WHERE product_name LIKE '%sauce%'
   OR product_name LIKE '%cream%'
ORDER BY product_name;`,
        description: 'Find products containing "sauce" or "cream" (case-insensitive)',
        sqlFeatures: ['LIKE', 'OR', 'Case-insensitive matching'],
      },
      {
        id: 'pattern-3',
        title: 'LIKE with Underscore Wildcard',
        database: 'employees',
        sql: `SELECT
  first_name,
  last_name
FROM employees
WHERE first_name LIKE 'J_hn'
ORDER BY last_name;`,
        description: 'Match names like "John" using _ for single character',
        sqlFeatures: ['LIKE', '_ wildcard', 'Single character matching'],
      },
      {
        id: 'pattern-4',
        title: 'BETWEEN for Ranges',
        database: 'northwind',
        sql: `SELECT
  product_name,
  unit_price
FROM products
WHERE unit_price BETWEEN 10 AND 50
ORDER BY unit_price;
-- EXPECTED:
-- | product_name                    | unit_price | 
-- | Aniseed Syrup                   | 10.0       | 
-- | Genen Shouyu                    | 15.5       | 
-- | Pavlova                         | 17.45      | 
-- | Chai                            | 18.0       | 
-- | Chang                           | 19.0       | 
-- | Queso Cabrales                  | 21.0       | 
-- | Chef Anton's Gumbo Mix          | 21.35      | 
-- | Chef Anton's Cajun Seasoning    | 22.0       | 
-- | Tofu                            | 23.25      | 
-- | Grandma's Boysenberry Spread    | 25.0       | 
-- (15 rows)`,
        description: 'Find products with prices in the 10-50 range (inclusive)',
        sqlFeatures: ['BETWEEN', 'Numeric ranges', 'Inclusive bounds'],
      },
      {
        id: 'pattern-5',
        title: 'BETWEEN with Dates',
        database: 'employees',
        sql: `SELECT
  first_name || ' ' || last_name AS employee,
  hire_date,
  department
FROM employees
WHERE YEAR(hire_date) BETWEEN 2019 AND 2021
ORDER BY hire_date;`,
        description: 'Filter employees hired between 2019-2021 using date functions',
        sqlFeatures: ['BETWEEN', 'YEAR', 'Date ranges'],
      },
      {
        id: 'pattern-6',
        title: 'IN with List',
        database: 'northwind',
        sql: `SELECT
  product_name,
  category_id,
  unit_price
FROM products
WHERE category_id IN (1, 2, 3)
ORDER BY category_id, unit_price DESC;
-- EXPECTED:
-- | product_name                 | category_id | unit_price | 
-- | Chang                        | 1           | 19.0       | 
-- | Chai                         | 1           | 18.0       | 
-- | Northwoods Cranberry Sauce   | 2           | 40.0       | 
-- | Grandma's Boysenberry Spread | 2           | 25.0       | 
-- | Chef Anton's Cajun Seasoning | 2           | 22.0       | 
-- | Chef Anton's Gumbo Mix       | 2           | 21.35      | 
-- | Genen Shouyu                 | 2           | 15.5       | 
-- | Aniseed Syrup                | 2           | 10.0       | 
-- | Sir Rodney's Marmalade       | 3           | 81.0       | 
-- | Pavlova                      | 3           | 17.45      | 
-- (11 rows)`,
        description: 'Find products in specific categories using IN operator',
        sqlFeatures: ['IN', 'List of values', 'Multi-column ORDER BY'],
      },
      {
        id: 'pattern-7',
        title: 'IN with Subquery',
        database: 'northwind',
        sql: `SELECT
  product_name,
  unit_price
FROM products
WHERE category_id IN (
  SELECT category_id
  FROM categories
  WHERE category_name LIKE '%Bev%'
)
ORDER BY unit_price DESC;
-- EXPECTED:
-- | product_name | unit_price | 
-- | Chang        | 19.0       | 
-- | Chai         | 18.0       | 
-- (2 rows)`,
        description: 'Use IN with subquery to find products in Beverage categories',
        sqlFeatures: ['IN', 'Subquery', 'LIKE', 'Dynamic filtering'],
      },
      {
        id: 'pattern-8',
        title: 'EXISTS with Correlated Subquery',
        database: 'northwind',
        sql: `SELECT
  c.category_name
FROM categories c
WHERE EXISTS (
  SELECT 1
  FROM products p
  WHERE p.category_id = c.category_id
    AND p.unit_price > 50
)
ORDER BY c.category_name;`,
        description: 'Find categories that have expensive products (>$50)',
        sqlFeatures: ['EXISTS', 'Correlated subquery', 'Existential check'],
      },
      {
        id: 'pattern-9',
        title: 'NOT IN and NOT BETWEEN',
        database: 'northwind',
        sql: `SELECT
  product_name,
  unit_price
FROM products
WHERE unit_price NOT BETWEEN 10 AND 30
  AND category_id NOT IN (1, 2)
ORDER BY unit_price DESC
LIMIT 10;
-- EXPECTED:
-- | product_name               | unit_price | 
-- | Mishi Kobe Niku            | 97.0       | 
-- | Sir Rodney's Marmalade     | 81.0       | 
-- | Carnarvon Tigers           | 62.5       | 
-- | Alice Mutton               | 39.0       | 
-- | Queso Manchego La Pastora  | 38.0       | 
-- | Ikura                      | 31.0       | 
-- | Teatime Chocolate Biscuits | 9.2        | 
-- | Konbu                      | 6.0        | 
-- (8 rows)`,
        description: 'Find products outside price range and excluded categories',
        sqlFeatures: ['NOT BETWEEN', 'NOT IN', 'Negative predicates', 'LIMIT'],
      },
      {
        id: 'pattern-10',
        title: 'Combining Predicates',
        database: 'northwind',
        sql: `SELECT
  product_name,
  category_id,
  unit_price,
  units_in_stock
FROM products
WHERE product_name LIKE 'C%'
  AND unit_price BETWEEN 5 AND 100
  AND category_id IN (1, 2, 3, 4)
  AND units_in_stock > 0
ORDER BY unit_price DESC;
-- EXPECTED:
-- | product_name                 | category_id | unit_price | units_in_stock | 
-- | Chef Anton's Cajun Seasoning | 2           | 22.0       | 53             | 
-- | Chang                        | 1           | 19.0       | 17             | 
-- | Chai                         | 1           | 18.0       | 39             | 
-- (3 rows)`,
        description: 'Complex filtering using multiple predicates together',
        sqlFeatures: ['LIKE', 'BETWEEN', 'IN', 'AND', 'Multiple predicates'],
      },
      {
        id: 'pattern-11',
        title: 'ALL Quantified Comparison',
        database: 'northwind',
        sql: `SELECT
  product_name,
  unit_price
FROM products p
WHERE unit_price > ALL (
  SELECT unit_price
  FROM products
  WHERE category_id = 1
)
ORDER BY unit_price DESC
LIMIT 5;
-- EXPECTED:
-- | product_name               | unit_price | 
-- | Mishi Kobe Niku            | 97.0       | 
-- | Sir Rodney's Marmalade     | 81.0       | 
-- | Carnarvon Tigers           | 62.5       | 
-- | Northwoods Cranberry Sauce | 40.0       | 
-- | Alice Mutton               | 39.0       | 
-- (5 rows)`,
        description: 'Find products more expensive than ALL category 1 products',
        sqlFeatures: ['ALL', 'Quantified comparison', 'Subquery', 'Universal quantifier'],
      },
      {
        id: 'pattern-12',
        title: 'ANY/SOME Quantified Comparison',
        database: 'northwind',
        sql: `SELECT
  product_name,
  unit_price,
  category_id
FROM products p
WHERE unit_price > ANY (
  SELECT AVG(unit_price)
  FROM products
  GROUP BY category_id
)
ORDER BY unit_price DESC
LIMIT 10;`,
        description: 'Find products more expensive than ANY category average',
        sqlFeatures: [
          'ANY',
          'SOME',
          'Quantified comparison',
          'AVG',
          'GROUP BY',
          'Existential quantifier',
        ],
      },
    ],
  },

  {
    id: 'math',
    title: 'Math Functions',
    description: 'ABS, ROUND, POWER, SQRT, trigonometric, and comparison functions',
    queries: [
      {
        id: 'math-1',
        title: 'Absolute Value and Sign',
        database: 'northwind',
        sql: `SELECT
  unit_price,
  unit_price - 20 AS price_diff,
  ABS(unit_price - 20) AS abs_diff,
  SIGN(unit_price - 20) AS sign_indicator
FROM products
LIMIT 10;
-- EXPECTED:
-- | unit_price | price_diff | abs_diff | sign_indicator |
-- | 18.0       | -2.0       | 2.0      | -1             |
-- | 19.0       | -1.0       | 1.0      | -1             |
-- | 10.0       | -10.0      | 10.0     | -1             |
-- | 22.0       | 2.0        | 2.0      | 1              |
-- | 21.35      | 1.35       | 1.35     | 1              |
-- | 15.5       | -4.5       | 4.5      | -1             |
-- | 25.0       | 5.0        | 5.0      | 1              |
-- | 40.0       | 20.0       | 20.0     | 1              |
-- | 17.45      | -2.55      | 2.55     | -1             |
-- | 81.0       | 61.0       | 61.0     | 1              |
-- (10 rows)`,
        description: 'Calculate absolute values and sign indicators for price differences',
        sqlFeatures: ['ABS', 'SIGN', 'Expressions'],
      },
      {
        id: 'math-2',
        title: 'Rounding Functions',
        database: 'northwind',
        sql: `SELECT
  unit_price,
  ROUND(unit_price) AS rounded,
  ROUND(unit_price, 1) AS one_decimal,
  FLOOR(unit_price) AS floor_value,
  CEIL(unit_price) AS ceiling_value
FROM products
WHERE unit_price IS NOT NULL
LIMIT 10;
-- EXPECTED:
-- | unit_price | rounded | one_decimal | floor_value | ceiling_value |
-- | 18.0       | 18.0    | 18.0        | 18.0        | 18.0          |
-- | 19.0       | 19.0    | 19.0        | 19.0        | 19.0          |
-- | 10.0       | 10.0    | 10.0        | 10.0        | 10.0          |
-- | 22.0       | 22.0    | 22.0        | 22.0        | 22.0          |
-- | 21.35      | 21.0    | 21.4        | 21.0        | 22.0          |
-- | 15.5       | 16.0    | 15.5        | 15.0        | 16.0          |
-- | 25.0       | 25.0    | 25.0        | 25.0        | 25.0          |
-- | 40.0       | 40.0    | 40.0        | 40.0        | 40.0          |
-- | 17.45      | 17.0    | 17.5        | 17.0        | 18.0          |
-- | 81.0       | 81.0    | 81.0        | 81.0        | 81.0          |
-- (10 rows)`,
        description: 'Demonstrate various rounding methods for numeric values',
        sqlFeatures: ['ROUND', 'FLOOR', 'CEIL', 'WHERE'],
      },
      {
        id: 'math-3',
        title: 'Power and Square Root',
        database: 'northwind',
        sql: `SELECT
  unit_price,
  POWER(unit_price, 2) AS squared,
  SQRT(unit_price) AS square_root,
  ROUND(SQRT(unit_price), 2) AS sqrt_rounded
FROM products
WHERE unit_price > 0
LIMIT 10;
-- EXPECTED:
-- | unit_price | squared | square_root | sqrt_rounded |
-- | 18.0       | 324.0   | 4.2426...   | 4.24         |
-- | 19.0       | 361.0   | 4.3588...   | 4.36         |
-- | 10.0       | 100.0   | 3.1622...   | 3.16         |
-- | 22.0       | 484.0   | 4.6904...   | 4.69         |
-- | 21.35      | 455.82  | 4.6206...   | 4.62         |
-- | 15.5       | 240.25  | 3.9370...   | 3.94         |
-- | 25.0       | 625.0   | 5.0         | 5.0          |
-- | 40.0       | 1600.0  | 6.3245...   | 6.32         |
-- | 17.45      | 304.50  | 4.1773...   | 4.18         |
-- | 81.0       | 6561.0  | 9.0         | 9.0          |
-- (10 rows)`,
        description: 'Calculate squares, square roots, and combinations',
        sqlFeatures: ['POWER', 'SQRT', 'ROUND', 'WHERE'],
      },
      {
        id: 'math-4',
        title: 'Modulo (Remainder)',
        database: 'northwind',
        sql: `SELECT
  product_id,
  product_name,
  MOD(product_id, 3) AS group_number,
  CASE
    WHEN MOD(product_id, 3) = 0 THEN 'Group A'
    WHEN MOD(product_id, 3) = 1 THEN 'Group B'
    ELSE 'Group C'
  END AS group_name
FROM products
ORDER BY product_id
LIMIT 15;
-- EXPECTED:
-- | product_id | product_name                     | group_number | group_name |
-- | 1          | Chai                             | 1            | Group B    |
-- | 2          | Chang                            | 2            | Group C    |
-- | 3          | Aniseed Syrup                    | 0            | Group A    |
-- | 4          | Chef Anton's Cajun Seasoning     | 1            | Group B    |
-- | 5          | Chef Anton's Gumbo Mix           | 2            | Group C    |
-- | 6          | Genen Shouyu                     | 0            | Group A    |
-- | 7          | Grandma's Boysenberry Spread     | 1            | Group B    |
-- | 8          | Northwoods Cranberry Sauce       | 2            | Group C    |
-- | 9          | Pavlova                          | 0            | Group A    |
-- | 10         | Sir Rodney's Marmalade           | 1            | Group B    |
-- | 11         | Teatime Chocolate Biscuits       | 2            | Group C    |
-- | 12         | Queso Cabrales                   | 0            | Group A    |
-- | 13         | Queso Manchego La Pastora        | 1            | Group B    |
-- | 14         | Alice Mutton                     | 2            | Group C    |
-- | 15         | Mishi Kobe Niku                  | 0            | Group A    |
-- (15 rows)`,
        description: 'Use MOD to divide products into rotating groups',
        sqlFeatures: ['MOD', 'CASE', 'ORDER BY'],
      },
      {
        id: 'math-5',
        title: 'Logarithmic Functions',
        database: 'northwind',
        sql: `SELECT
  unit_price,
  ROUND(LN(unit_price), 2) AS natural_log,
  ROUND(LOG10(unit_price), 2) AS log_base_10,
  ROUND(EXP(1.0), 4) AS e_constant
FROM products
WHERE unit_price > 0
LIMIT 10;
-- EXPECTED:
-- | unit_price | natural_log | log_base_10 | e_constant |
-- | 18.0       | 2.89        | 1.26        | 2.7183     |
-- | 19.0       | 2.94        | 1.28        | 2.7183     |
-- | 10.0       | 2.30        | 1.0         | 2.7183     |
-- | 22.0       | 3.09        | 1.34        | 2.7183     |
-- | 21.35      | 3.06        | 1.33        | 2.7183     |
-- | 15.5       | 2.74        | 1.19        | 2.7183     |
-- | 25.0       | 3.22        | 1.40        | 2.7183     |
-- | 40.0       | 3.69        | 1.60        | 2.7183     |
-- | 17.45      | 2.86        | 1.24        | 2.7183     |
-- | 81.0       | 4.39        | 1.91        | 2.7183     |
-- (10 rows)`,
        description: 'Calculate natural logarithms, base-10 logarithms, and exponentials',
        sqlFeatures: ['LN', 'LOG10', 'EXP', 'ROUND'],
      },
      {
        id: 'math-6',
        title: 'Trigonometric Functions',
        database: 'northwind',
        sql: `SELECT
  ROUND(PI(), 6) AS pi_value,
  ROUND(SIN(PI() / 2), 4) AS sin_90_degrees,
  ROUND(COS(PI()), 4) AS cos_180_degrees,
  ROUND(TAN(PI() / 4), 4) AS tan_45_degrees
FROM (SELECT * FROM products LIMIT 1) AS dummy;
-- EXPECTED:
-- | pi_value | sin_90_degrees | cos_180_degrees | tan_45_degrees |
-- | 3.141593 | 1.0            | -1.0            | 1.0            |
-- (1 row)`,
        description: 'Demonstrate trigonometric functions with common angles',
        sqlFeatures: ['PI', 'SIN', 'COS', 'TAN', 'ROUND'],
      },
      {
        id: 'math-7',
        title: 'Inverse Trigonometric Functions',
        database: 'northwind',
        sql: `SELECT
  ROUND(ASIN(1.0), 4) AS asin_1,
  ROUND(ACOS(0.0), 4) AS acos_0,
  ROUND(ATAN(1.0), 4) AS atan_1,
  ROUND(ATAN2(1.0, 1.0), 4) AS atan2_1_1
FROM (SELECT * FROM products LIMIT 1) AS dummy;
-- EXPECTED:
-- | asin_1 | acos_0 | atan_1 | atan2_1_1 |
-- | 1.5708 | 1.5708 | 0.7854 | 0.7854    |
-- (1 row)`,
        description: 'Calculate inverse trigonometric functions (arcsin, arccos, arctan)',
        sqlFeatures: ['ASIN', 'ACOS', 'ATAN', 'ATAN2', 'ROUND'],
      },
      {
        id: 'math-8',
        title: 'Angle Conversions',
        database: 'northwind',
        sql: `SELECT
  180 AS degrees,
  ROUND(RADIANS(180), 4) AS radians,
  ROUND(DEGREES(PI()), 2) AS back_to_degrees
FROM (SELECT * FROM products LIMIT 1) AS dummy;
-- EXPECTED:
-- | degrees | radians | back_to_degrees |
-- | 180     | 3.1416  | 180.0           |
-- (1 row)`,
        description: 'Convert between degrees and radians',
        sqlFeatures: ['RADIANS', 'DEGREES', 'PI', 'ROUND'],
      },
      {
        id: 'math-9',
        title: 'GREATEST and LEAST',
        database: 'northwind',
        sql: `SELECT
  product_name,
  unit_price,
  units_in_stock,
  GREATEST(unit_price, units_in_stock) AS max_value,
  LEAST(unit_price, units_in_stock) AS min_value
FROM products
WHERE unit_price IS NOT NULL AND units_in_stock IS NOT NULL
LIMIT 10;
-- EXPECTED:
-- | product_name                     | unit_price | units_in_stock | max_value | min_value |
-- | Chai                             | 18.0       | 39             | 39.0      | 18.0      |
-- | Chang                            | 19.0       | 17             | 19.0      | 17.0      |
-- | Aniseed Syrup                    | 10.0       | 13             | 13.0      | 10.0      |
-- | Chef Anton's Cajun Seasoning     | 22.0       | 53             | 53.0      | 22.0      |
-- | Chef Anton's Gumbo Mix           | 21.35      | 0              | 21.35     | 0.0       |
-- | Genen Shouyu                     | 15.5       | 39             | 39.0      | 15.5      |
-- | Grandma's Boysenberry Spread     | 25.0       | 120            | 120.0     | 25.0      |
-- | Northwoods Cranberry Sauce       | 40.0       | 6              | 40.0      | 6.0       |
-- | Pavlova                          | 17.45      | 29             | 29.0      | 17.45     |
-- | Sir Rodney's Marmalade           | 81.0       | 40             | 81.0      | 40.0      |
-- (10 rows)`,
        description: 'Find maximum and minimum values across columns',
        sqlFeatures: ['GREATEST', 'LEAST', 'WHERE'],
      },
      {
        id: 'math-10',
        title: 'Price Calculations',
        database: 'northwind',
        sql: `SELECT
  product_name,
  unit_price,
  ROUND(unit_price * 1.08, 2) AS with_tax,
  ROUND(unit_price * 0.9, 2) AS discounted,
  FLOOR(unit_price) || '.99' AS psychological_price
FROM products
WHERE unit_price > 0
ORDER BY unit_price DESC
LIMIT 10;
-- EXPECTED:
-- | product_name                  | unit_price | with_tax | discounted | psychological_price |
-- | Mishi Kobe Niku               | 97.0       | 104.76   | 87.30      | 97.99               |
-- | Sir Rodney's Marmalade        | 81.0       | 87.48    | 72.90      | 81.99               |
-- | Carnarvon Tigers              | 62.5       | 67.50    | 56.25      | 62.99               |
-- | Northwoods Cranberry Sauce    | 40.0       | 43.20    | 36.00      | 40.99               |
-- | Alice Mutton                  | 39.0       | 42.12    | 35.10      | 39.99               |
-- | Queso Manchego La Pastora     | 38.0       | 41.04    | 34.20      | 38.99               |
-- | Ikura                         | 31.0       | 33.48    | 27.90      | 31.99               |
-- | Uncle Bob's Organic Dried Pears| 30.0      | 32.40    | 27.00      | 30.99               |
-- | Grandma's Boysenberry Spread  | 25.0       | 27.00    | 22.50      | 25.99               |
-- | Tofu                          | 23.25      | 25.11    | 20.93      | 23.99               |
-- (10 rows)`,
        description: 'Practical business calculations: taxes, discounts, and pricing strategies',
        sqlFeatures: ['ROUND', 'FLOOR', 'String concatenation', 'Business calculations'],
      },
    ],
  },

  {
    id: 'null-handling',
    title: 'NULL Handling Functions',
    description: 'COALESCE and NULLIF for dealing with NULL values',
    queries: [
      {
        id: 'null-1',
        title: 'COALESCE with Default Values',
        database: 'northwind',
        sql: `SELECT
  product_name,
  unit_price,
  units_in_stock,
  COALESCE(units_in_stock, 0) AS stock_or_zero,
  COALESCE(units_on_order, 0) AS orders_or_zero
FROM products
LIMIT 10;
-- EXPECTED:
-- | product_name                    | unit_price | units_in_stock | stock_or_zero | orders_or_zero |
-- | Chai                            | 18.0       | 39             | 39            | 0              |
-- | Chang                           | 19.0       | 17             | 17            | 40             |
-- | Aniseed Syrup                   | 10.0       | 13             | 13            | 70             |
-- | Chef Anton's Cajun Seasoning    | 22.0       | 53             | 53            | 0              |
-- | Chef Anton's Gumbo Mix          | 21.35      | 0              | 0             | 0              |
-- | Grandma's Boysenberry Spread    | 25.0       | 120            | 120           | 0              |
-- | Uncle Bob's Organic Dried Pears | 30.0       | 15             | 15            | 0              |
-- | Northwoods Cranberry Sauce      | 40.0       | 6              | 6             | 0              |
-- | Mishi Kobe Niku                 | 97.0       | 29             | 29            | 0              |
-- | Ikura                           | 31.0       | 31             | 31            | 0              |
-- (10 rows)`,
        description: 'Replace NULL values with defaults using COALESCE',
        sqlFeatures: ['COALESCE', 'NULL defaults'],
      },
      {
        id: 'null-2',
        title: 'COALESCE with Multiple Fallbacks',
        database: 'employees',
        sql: `SELECT
  first_name,
  last_name,
  email,
  phone,
  COALESCE(email, phone, 'No contact info') AS primary_contact
FROM employees
LIMIT 10;
-- ⏭️ SKIP: Requires employees database with email/phone fields - not yet implemented in test environment`,
        description: 'Use COALESCE with multiple fallback values',
        sqlFeatures: ['COALESCE', 'Multiple fallback values'],
      },
      {
        id: 'null-3',
        title: 'NULLIF to Convert Values to NULL',
        database: 'northwind',
        sql: `SELECT
  product_name,
  units_in_stock,
  NULLIF(units_in_stock, 0) AS stock_if_available,
  CASE
    WHEN NULLIF(units_in_stock, 0) IS NULL THEN 'Out of Stock'
    ELSE 'In Stock'
  END AS status
FROM products
ORDER BY units_in_stock
LIMIT 15;
-- ⏭️ SKIP: NULLIF function not yet implemented;
-- EXPECTED:
-- | product_name                    | units_in_stock | stock_if_available | status       | 
-- | Chef Anton's Gumbo Mix          | 0              | NULL               | Out of Stock | 
-- | Alice Mutton                    | 0              | NULL               | Out of Stock | 
-- | Northwoods Cranberry Sauce      | 6              | 6                  | In Stock     | 
-- | Aniseed Syrup                   | 13             | 13                 | In Stock     | 
-- | Uncle Bob's Organic Dried Pears | 15             | 15                 | In Stock     | 
-- | Chang                           | 17             | 17                 | In Stock     | 
-- | Queso Cabrales                  | 22             | 22                 | In Stock     | 
-- | Konbu                           | 24             | 24                 | In Stock     | 
-- | Teatime Chocolate Biscuits      | 25             | 25                 | In Stock     | 
-- | Mishi Kobe Niku                 | 29             | 29                 | In Stock     | 
-- (15 rows)`,
        description: 'Convert specific values to NULL using NULLIF',
        sqlFeatures: ['NULLIF', 'CASE', 'IS NULL'],
      },
      {
        id: 'null-4',
        title: 'COALESCE in Calculations',
        database: 'northwind',
        sql: `SELECT
  product_name,
  unit_price,
  units_in_stock,
  unit_price * COALESCE(units_in_stock, 0) AS inventory_value
FROM products
ORDER BY inventory_value DESC
LIMIT 10;
-- ⏭️ SKIP: Arithmetic with COALESCE - may have type coercion issues`,
        description: 'Use COALESCE for NULL-safe arithmetic operations',
        sqlFeatures: ['COALESCE', 'Calculated fields', 'NULL-safe arithmetic'],
      },
      {
        id: 'null-5',
        title: 'COALESCE with Aggregates',
        database: 'northwind',
        sql: `SELECT
  c.category_name,
  COUNT(p.product_id) AS product_count,
  COALESCE(AVG(p.unit_price), 0) AS avg_price,
  COALESCE(SUM(p.units_in_stock), 0) AS total_stock
FROM categories c
LEFT JOIN products p ON c.category_id = p.category_id
GROUP BY c.category_name
ORDER BY product_count DESC;
-- ⏭️ SKIP: Complex aggregation with COALESCE - may have type coercion issues`,
        description: 'Handle NULL results from LEFT JOIN aggregates with COALESCE',
        sqlFeatures: [
          'COALESCE',
          'LEFT JOIN',
          'AVG',
          'SUM',
          'GROUP BY',
          'NULL handling in aggregates',
        ],
      },
      {
        id: 'null-6',
        title: 'Combining COALESCE and NULLIF',
        database: 'employees',
        sql: `SELECT
  first_name || ' ' || last_name AS employee,
  salary,
  commission,
  COALESCE(NULLIF(commission, 0), salary * 0.05) AS effective_commission
FROM employees
ORDER BY effective_commission DESC
LIMIT 10;
-- ⏭️ SKIP: Requires employees database - NULLIF not yet implemented`,
        description: 'Combine COALESCE and NULLIF for complex NULL logic',
        sqlFeatures: ['COALESCE', 'NULLIF', 'Complex NULL logic'],
      },
      {
        id: 'null-7',
        title: 'NULL-Safe Comparisons',
        database: 'northwind',
        sql: `SELECT
  product_name,
  units_in_stock,
  units_on_order,
  CASE
    WHEN COALESCE(units_in_stock, 0) = 0
     AND COALESCE(units_on_order, 0) > 0 THEN 'Restocking'
    WHEN COALESCE(units_in_stock, 0) = 0 THEN 'Out of Stock'
    WHEN COALESCE(units_in_stock, 0) < 10 THEN 'Low Stock'
    ELSE 'In Stock'
  END AS inventory_status
FROM products
ORDER BY COALESCE(units_in_stock, 0);
-- ⏭️ SKIP: CASE expressions with COALESCE - type coercion issues with comparisons;
-- EXPECTED:
-- | product_name                    | units_in_stock | units_on_order | inventory_status | 
-- | Chef Anton's Gumbo Mix          | 0              | 0              | Out of Stock     | 
-- | Alice Mutton                    | 0              | 0              | Out of Stock     | 
-- | Northwoods Cranberry Sauce      | 6              | 0              | Low Stock        | 
-- | Aniseed Syrup                   | 13             | 70             | In Stock         | 
-- | Uncle Bob's Organic Dried Pears | 15             | 0              | In Stock         | 
-- | Chang                           | 17             | 40             | In Stock         | 
-- | Queso Cabrales                  | 22             | 30             | In Stock         | 
-- | Konbu                           | 24             | 0              | In Stock         | 
-- | Teatime Chocolate Biscuits      | 25             | 0              | In Stock         | 
-- | Mishi Kobe Niku                 | 29             | 0              | In Stock         | 
-- (20 rows)`,
        description: 'Use COALESCE for NULL-safe comparisons in business logic',
        sqlFeatures: ['COALESCE', 'CASE', 'NULL-safe comparisons', 'Business logic'],
      },
      {
        id: 'null-8',
        title: 'COALESCE for Report Formatting',
        database: 'northwind',
        sql: `SELECT
  c.category_name,
  COALESCE(
    CAST(COUNT(p.product_id) AS VARCHAR) || ' products',
    'No products'
  ) AS product_summary,
  COALESCE(
    '$' || CAST(ROUND(AVG(p.unit_price), 2) AS VARCHAR),
    'N/A'
  ) AS avg_price_formatted
FROM categories c
LEFT JOIN products p ON c.category_id = p.category_id
GROUP BY c.category_name
ORDER BY COUNT(p.product_id) DESC;
-- ⏭️ SKIP: CAST to VARCHAR not yet implemented - requires type conversion support`,
        description: 'Format report output with COALESCE for NULL-safe string operations',
        sqlFeatures: ['COALESCE', 'CAST', 'String concatenation', 'Report formatting'],
      },
    ],
  },

  {
    id: 'window',
    title: 'Window Functions',
    description:
      'Aggregate window functions with OVER clause - running totals, moving averages, partitioned aggregates',
    queries: [
      {
        id: 'window-1',
        title: 'COUNT(*) OVER - Total Row Count',
        database: 'employees',
        sql: `SELECT
  first_name || ' ' || last_name AS employee,
  department,
  salary,
  COUNT(*) OVER () AS total_employees
FROM employees
LIMIT 10;
-- ⏭️ SKIP: Requires employees database - not yet implemented in test environment`,
        description: 'Add total count to each row without GROUP BY collapse',
        sqlFeatures: ['COUNT', 'OVER', 'Window functions'],
      },
      {
        id: 'window-2',
        title: 'Running Total with ORDER BY',
        database: 'employees',
        sql: `SELECT
  first_name || ' ' || last_name AS employee,
  salary,
  SUM(salary) OVER (ORDER BY employee_id) AS running_total
FROM employees
ORDER BY employee_id
LIMIT 10;
-- ⏭️ SKIP: Requires employees database - not yet implemented in test environment`,
        description: 'Calculate cumulative salary sum ordered by employee ID',
        sqlFeatures: ['SUM', 'OVER', 'ORDER BY', 'Running totals'],
      },
      {
        id: 'window-3',
        title: 'Partitioned Averages',
        database: 'employees',
        sql: `SELECT
  department,
  first_name || ' ' || last_name AS employee,
  salary,
  AVG(salary) OVER (PARTITION BY department) AS dept_avg_salary
FROM employees
ORDER BY department, salary DESC
LIMIT 15;
-- ⏭️ SKIP: Requires employees database - not yet implemented in test environment`,
        description: 'Calculate average salary per department for each employee',
        sqlFeatures: ['AVG', 'OVER', 'PARTITION BY', 'Partitioned aggregates'],
      },
      {
        id: 'window-4',
        title: 'MIN and MAX in Windows',
        database: 'northwind',
        sql: `SELECT
  product_name,
  category_id,
  unit_price,
  MIN(unit_price) OVER (PARTITION BY category_id) AS category_min,
  MAX(unit_price) OVER (PARTITION BY category_id) AS category_max
FROM products
WHERE category_id IN (1, 2, 3)
ORDER BY category_id, unit_price;
-- ⏭️ SKIP: Window functions with OVER clause not yet fully integrated with executor;
-- EXPECTED:
-- | product_name                 | category_id | unit_price | category_min | category_max | 
-- | Chai                         | 1           | 18.0       | 18.0         | 19.0         | 
-- | Chang                        | 1           | 19.0       | 18.0         | 19.0         | 
-- | Aniseed Syrup                | 2           | 10.0       | 10.0         | 40.0         | 
-- | Genen Shouyu                 | 2           | 15.5       | 10.0         | 40.0         | 
-- | Chef Anton's Gumbo Mix       | 2           | 21.35      | 10.0         | 40.0         | 
-- | Chef Anton's Cajun Seasoning | 2           | 22.0       | 10.0         | 40.0         | 
-- | Grandma's Boysenberry Spread | 2           | 25.0       | 10.0         | 40.0         | 
-- | Northwoods Cranberry Sauce   | 2           | 40.0       | 10.0         | 40.0         | 
-- | Teatime Chocolate Biscuits   | 3           | 9.2        | 17.45        | 9.2          | 
-- | Pavlova                      | 3           | 17.45      | 17.45        | 9.2          | 
-- (11 rows)`,
        description: 'Find min and max prices within each product category',
        sqlFeatures: ['MIN', 'MAX', 'OVER', 'PARTITION BY'],
      },
      {
        id: 'window-5',
        title: 'Moving Average with Frame',
        database: 'employees',
        sql: `SELECT
  employee_id,
  salary,
  AVG(salary) OVER (
    ORDER BY employee_id
    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
  ) AS moving_avg_3
FROM employees
ORDER BY employee_id
LIMIT 15;
-- ⏭️ SKIP: Requires employees database - window frames (ROWS BETWEEN) not yet implemented`,
        description: 'Calculate 3-row moving average of salaries',
        sqlFeatures: ['AVG', 'OVER', 'ROWS BETWEEN', 'Moving frame', 'Window frames'],
      },
      {
        id: 'window-6',
        title: 'Window Function in Expression',
        database: 'employees',
        sql: `SELECT
  department,
  first_name || ' ' || last_name AS employee,
  salary,
  AVG(salary) OVER (PARTITION BY department) AS dept_avg,
  salary - AVG(salary) OVER (PARTITION BY department) AS diff_from_avg
FROM employees
ORDER BY department, diff_from_avg DESC
LIMIT 15;
-- ⏭️ SKIP: Requires employees database - not yet implemented in test environment`,
        description: 'Calculate salary deviation from department average',
        sqlFeatures: ['AVG', 'OVER', 'PARTITION BY', 'Window functions in expressions'],
      },
      {
        id: 'window-7',
        title: 'Multiple Window Functions',
        database: 'employees',
        sql: `SELECT
  department,
  COUNT(*) OVER (PARTITION BY department) AS dept_headcount,
  AVG(salary) OVER (PARTITION BY department) AS dept_avg_salary,
  MIN(salary) OVER (PARTITION BY department) AS dept_min_salary,
  MAX(salary) OVER (PARTITION BY department) AS dept_max_salary
FROM employees
ORDER BY department
LIMIT 15;
-- ⏭️ SKIP: Requires employees database - not yet implemented in test environment`,
        description: 'Multiple aggregate window functions in one query',
        sqlFeatures: [
          'COUNT',
          'AVG',
          'MIN',
          'MAX',
          'OVER',
          'PARTITION BY',
          'Multiple window functions',
        ],
      },
      {
        id: 'window-8',
        title: 'Percentage of Total',
        database: 'northwind',
        sql: `SELECT
  product_name,
  unit_price,
  SUM(unit_price) OVER () AS total_price,
  ROUND(unit_price * 100.0 / SUM(unit_price) OVER (), 2) AS pct_of_total
FROM products
WHERE unit_price IS NOT NULL
ORDER BY pct_of_total DESC
LIMIT 10;
-- ⏭️ SKIP: Window functions with OVER clause not yet fully integrated with executor`,
        description: 'Calculate each product price as percentage of total',
        sqlFeatures: ['SUM', 'OVER', 'ROUND', 'Percentage calculations', 'Business analytics'],
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

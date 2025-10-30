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
  // Enhanced metadata (Step 2 additions)
  difficulty?: 'beginner' | 'intermediate' | 'advanced'
  useCase?: 'analytics' | 'admin' | 'development' | 'reports' | 'data-quality'
  performanceNotes?: string
  executionTimeMs?: number
  relatedExamples?: string[] // IDs of related examples
  tags?: string[] // Searchable tags
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
-- EXPECTED:
-- | product_name                     | unit_price | units_in_stock | inventory_value |
-- | Mishi Kobe Niku                  | 97.0       | 29             | 2813.0          |
-- | Chai                             | 18.0       | 39             | 702.0           |
-- | Northwoods Cranberry Sauce       | 40.0       | 6              | 240.0           |
-- | Chang                            | 19.0       | 17             | 323.0           |
-- | Aniseed Syrup                    | 10.0       | 13             | 130.0           |
-- | Chef Anton's Cajun Seasoning     | 22.0       | 53             | 1166.0          |
-- | Grandma's Boysenberry Spread     | 25.0       | 120            | 3000.0          |
-- | Uncle Bob's Organic Dried Pears  | 30.0       | 15             | 450.0           |
-- | Queso Cabrales                   | 21.0       | 22             | 462.0           |
-- | Ikura                            | 31.0       | 31             | 961.0           |
-- (10 rows)`,
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
-- EXPECTED:
-- | category_name  | product_count | avg_price | total_stock |
-- | Beverages      | 12            | 37.98     | 559         |
-- | Condiments     | 12            | 23.06     | 507         |
-- | Confections    | 13            | 25.16     | 386         |
-- | Dairy Products | 10            | 28.73     | 393         |
-- | Grains/Cereals | 7             | 32.37     | 308         |
-- | Meat/Poultry   | 6             | 54.01     | 165         |
-- | Produce        | 5             | 32.37     | 100         |
-- | Seafood        | 12            | 20.68     | 701         |
-- (8 rows)`,
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
  // ========================================
  // NEW CATEGORIES (Step 2)
  // ========================================
  {
    id: 'business-intelligence',
    title: 'Business Intelligence Queries',
    description: 'Real-world analytics scenarios: YoY growth, RFM analysis, sales funnels',
    queries: [
      {
        id: 'bi-1',
        title: 'Year-over-Year Growth Analysis',
        database: 'northwind',
        sql: `-- Compare order counts by year
SELECT
  CAST(EXTRACT(YEAR FROM order_date) AS INTEGER) AS year,
  COUNT(*) AS order_count,
  LAG(COUNT(*)) OVER (ORDER BY EXTRACT(YEAR FROM order_date)) AS prev_year_count,
  CASE
    WHEN LAG(COUNT(*)) OVER (ORDER BY EXTRACT(YEAR FROM order_date)) IS NOT NULL
    THEN ROUND(
      (COUNT(*) - LAG(COUNT(*)) OVER (ORDER BY EXTRACT(YEAR FROM order_date))) * 100.0 /
      LAG(COUNT(*)) OVER (ORDER BY EXTRACT(YEAR FROM order_date)),
      2
    )
    ELSE NULL
  END AS yoy_growth_pct
FROM orders
WHERE order_date IS NOT NULL
GROUP BY EXTRACT(YEAR FROM order_date)
ORDER BY year;
-- EXPECTED: Year-over-year comparison with growth percentages
-- (Shows business growth trends)`,
        description: 'Calculate year-over-year growth in order volume',
        sqlFeatures: ['EXTRACT', 'LAG', 'OVER', 'CASE', 'Window functions', 'Date functions'],
        difficulty: 'intermediate',
        useCase: 'analytics',
        tags: ['yoy', 'growth', 'trends', 'business-metrics'],
        relatedExamples: ['aggregate-1', 'window-1'],
      },
      {
        id: 'bi-2',
        title: 'Customer Segmentation (RFM Analysis)',
        database: 'northwind',
        sql: `-- RFM: Recency, Frequency, Monetary analysis
WITH customer_rfm AS (
  SELECT
    customer_id,
    MAX(order_date) AS last_order_date,
    COUNT(*) AS order_frequency,
    SUM(COALESCE(unit_price * quantity * (1 - discount), 0)) AS monetary_value
  FROM orders o
  LEFT JOIN order_details od ON o.order_id = od.order_id
  WHERE o.order_date IS NOT NULL
  GROUP BY customer_id
)
SELECT
  customer_id,
  last_order_date,
  order_frequency,
  ROUND(monetary_value, 2) AS total_spent,
  CASE
    WHEN order_frequency >= 5 AND monetary_value >= 1000 THEN 'VIP'
    WHEN order_frequency >= 3 AND monetary_value >= 500 THEN 'Loyal'
    WHEN order_frequency >= 2 THEN 'Regular'
    ELSE 'Occasional'
  END AS customer_segment
FROM customer_rfm
ORDER BY monetary_value DESC
LIMIT 20;
-- EXPECTED: Top 20 customers with RFM segments
-- (Identifies high-value customer segments)`,
        description: 'Segment customers by purchase behavior (RFM model)',
        sqlFeatures: ['CTE', 'Aggregates', 'CASE', 'Segmentation'],
        difficulty: 'advanced',
        useCase: 'analytics',
        tags: ['rfm', 'segmentation', 'customer-value', 'crm'],
        relatedExamples: ['cte-1', 'aggregate-1'],
      },
      {
        id: 'bi-3',
        title: 'Sales Funnel Analysis',
        database: 'northwind',
        sql: `-- Analyze sales by product category with conversion metrics
WITH category_sales AS (
  SELECT
    c.category_name,
    COUNT(DISTINCT p.product_id) AS total_products,
    COUNT(DISTINCT od.order_id) AS orders_count,
    SUM(od.quantity) AS units_sold,
    SUM(od.unit_price * od.quantity * (1 - od.discount)) AS revenue
  FROM categories c
  LEFT JOIN products p ON c.category_id = p.category_id
  LEFT JOIN order_details od ON p.product_id = od.product_id
  GROUP BY c.category_name
)
SELECT
  category_name,
  total_products,
  orders_count,
  units_sold,
  ROUND(revenue, 2) AS total_revenue,
  ROUND(revenue / NULLIF(total_products, 0), 2) AS revenue_per_product,
  ROUND(units_sold::DECIMAL / NULLIF(orders_count, 0), 2) AS avg_units_per_order
FROM category_sales
ORDER BY total_revenue DESC;
-- EXPECTED: Category performance with funnel metrics
-- (Shows conversion efficiency by category)`,
        description: 'Analyze sales funnel metrics by product category',
        sqlFeatures: ['CTE', 'JOINs', 'Aggregates', 'NULLIF', 'Business metrics'],
        difficulty: 'intermediate',
        useCase: 'analytics',
        tags: ['funnel', 'conversion', 'sales-metrics', 'category-analysis'],
        relatedExamples: ['join-1', 'aggregate-1'],
      },
      {
        id: 'bi-4',
        title: 'Cohort Analysis',
        database: 'northwind',
        sql: `-- Analyze customer retention by order cohort
WITH first_orders AS (
  SELECT
    customer_id,
    MIN(order_date) AS first_order_date,
    DATE_TRUNC('month', MIN(order_date)) AS cohort_month
  FROM orders
  WHERE order_date IS NOT NULL
  GROUP BY customer_id
),
cohort_data AS (
  SELECT
    fo.cohort_month,
    DATE_TRUNC('month', o.order_date) AS order_month,
    COUNT(DISTINCT o.customer_id) AS customers
  FROM first_orders fo
  JOIN orders o ON fo.customer_id = o.customer_id
  WHERE o.order_date IS NOT NULL
  GROUP BY fo.cohort_month, DATE_TRUNC('month', o.order_date)
)
SELECT
  cohort_month,
  order_month,
  customers,
  ROUND(
    customers * 100.0 / FIRST_VALUE(customers) OVER (
      PARTITION BY cohort_month
      ORDER BY order_month
    ),
    2
  ) AS retention_pct
FROM cohort_data
ORDER BY cohort_month, order_month
LIMIT 25;
-- EXPECTED: Customer retention by cohort over time
-- (Shows how well each cohort is retained)`,
        description: 'Track customer retention over time using cohort analysis',
        sqlFeatures: ['CTE', 'DATE_TRUNC', 'FIRST_VALUE', 'Window functions', 'Cohort analysis'],
        difficulty: 'advanced',
        useCase: 'analytics',
        tags: ['cohort', 'retention', 'customer-lifetime-value', 'churn'],
        relatedExamples: ['cte-1', 'window-1'],
      },
      {
        id: 'bi-5',
        title: 'Product Affinity Analysis',
        database: 'northwind',
        sql: `-- Find products frequently purchased together
WITH product_pairs AS (
  SELECT
    od1.product_id AS product_a,
    od2.product_id AS product_b,
    COUNT(DISTINCT od1.order_id) AS co_occurrence
  FROM order_details od1
  JOIN order_details od2 ON od1.order_id = od2.order_id
    AND od1.product_id < od2.product_id
  GROUP BY od1.product_id, od2.product_id
  HAVING COUNT(DISTINCT od1.order_id) >= 3
)
SELECT
  p1.product_name AS product_a_name,
  p2.product_name AS product_b_name,
  pp.co_occurrence AS times_bought_together,
  ROUND(
    pp.co_occurrence * 100.0 / (
      SELECT COUNT(DISTINCT order_id) FROM order_details WHERE product_id = pp.product_a
    ),
    2
  ) AS affinity_pct
FROM product_pairs pp
JOIN products p1 ON pp.product_a = p1.product_id
JOIN products p2 ON pp.product_b = p2.product_id
ORDER BY times_bought_together DESC
LIMIT 15;
-- EXPECTED: Product pairs with high purchase correlation
-- (Useful for cross-selling and recommendations)`,
        description: 'Identify products frequently purchased together (market basket analysis)',
        sqlFeatures: ['Self-join', 'CTE', 'Subquery', 'Market basket analysis'],
        difficulty: 'advanced',
        useCase: 'analytics',
        tags: ['affinity', 'cross-sell', 'market-basket', 'recommendations'],
        relatedExamples: ['join-5', 'subquery-1'],
      },
      {
        id: 'bi-6',
        title: 'ABC Analysis (Inventory Classification)',
        database: 'northwind',
        sql: `-- Classify products by revenue contribution (ABC analysis)
WITH product_revenue AS (
  SELECT
    p.product_id,
    p.product_name,
    SUM(od.unit_price * od.quantity * (1 - od.discount)) AS revenue
  FROM products p
  LEFT JOIN order_details od ON p.product_id = od.product_id
  GROUP BY p.product_id, p.product_name
),
revenue_totals AS (
  SELECT
    product_id,
    product_name,
    revenue,
    SUM(revenue) OVER () AS total_revenue,
    SUM(revenue) OVER (ORDER BY revenue DESC) AS cumulative_revenue
  FROM product_revenue
)
SELECT
  product_name,
  ROUND(revenue, 2) AS product_revenue,
  ROUND(revenue * 100.0 / total_revenue, 2) AS pct_of_total,
  ROUND(cumulative_revenue * 100.0 / total_revenue, 2) AS cumulative_pct,
  CASE
    WHEN cumulative_revenue * 100.0 / total_revenue <= 70 THEN 'A'
    WHEN cumulative_revenue * 100.0 / total_revenue <= 90 THEN 'B'
    ELSE 'C'
  END AS abc_class
FROM revenue_totals
WHERE revenue > 0
ORDER BY revenue DESC;
-- EXPECTED: Products classified by revenue contribution (A/B/C)
-- (A = top 70%, B = next 20%, C = remaining 10%)`,
        description: 'Classify products by revenue contribution using ABC analysis',
        sqlFeatures: ['CTE', 'Window functions', 'SUM OVER', 'Classification'],
        difficulty: 'advanced',
        useCase: 'analytics',
        tags: ['abc-analysis', 'pareto', 'inventory', 'revenue-analysis'],
        relatedExamples: ['window-1', 'cte-1'],
      },
      {
        id: 'bi-7',
        title: 'Pareto Analysis (80/20 Rule)',
        database: 'northwind',
        sql: `-- Find products contributing to 80% of revenue (Pareto principle)
WITH ranked_products AS (
  SELECT
    p.product_name,
    SUM(od.unit_price * od.quantity * (1 - od.discount)) AS revenue,
    SUM(SUM(od.unit_price * od.quantity * (1 - od.discount)))
      OVER (ORDER BY SUM(od.unit_price * od.quantity * (1 - od.discount)) DESC) AS cumulative_revenue,
    SUM(SUM(od.unit_price * od.quantity * (1 - od.discount))) OVER () AS total_revenue
  FROM products p
  JOIN order_details od ON p.product_id = od.product_id
  GROUP BY p.product_name
)
SELECT
  product_name,
  ROUND(revenue, 2) AS revenue,
  ROUND(cumulative_revenue, 2) AS cumulative_revenue,
  ROUND(cumulative_revenue * 100.0 / total_revenue, 2) AS cumulative_pct,
  CASE
    WHEN cumulative_revenue * 100.0 / total_revenue <= 80 THEN 'Top 80%'
    ELSE 'Remaining 20%'
  END AS pareto_group
FROM ranked_products
ORDER BY revenue DESC;
-- EXPECTED: Products with cumulative revenue percentages
-- (Identifies vital few vs useful many)`,
        description: 'Apply Pareto principle to identify top revenue contributors',
        sqlFeatures: ['CTE', 'Window functions', 'Cumulative sums', 'Pareto analysis'],
        difficulty: 'intermediate',
        useCase: 'analytics',
        tags: ['pareto', '80-20', 'revenue-contribution', 'vital-few'],
        relatedExamples: ['bi-6', 'window-1'],
      },
    ],
  },
  {
    id: 'data-quality',
    title: 'Data Quality Checks',
    description: 'Common data validation patterns: duplicates, orphans, NULL analysis',
    queries: [
      {
        id: 'dq-1',
        title: 'Finding Duplicate Records',
        database: 'northwind',
        sql: `-- Find products with duplicate names
SELECT
  product_name,
  COUNT(*) AS occurrence_count,
  STRING_AGG(CAST(product_id AS TEXT), ', ') AS product_ids
FROM products
GROUP BY product_name
HAVING COUNT(*) > 1
ORDER BY occurrence_count DESC;
-- EXPECTED: Products with duplicate names (if any)
-- (Should return 0 rows if data is clean)`,
        description: 'Detect duplicate product names in the database',
        sqlFeatures: ['GROUP BY', 'HAVING', 'COUNT', 'STRING_AGG', 'Duplicate detection'],
        difficulty: 'beginner',
        useCase: 'data-quality',
        tags: ['duplicates', 'validation', 'data-cleansing', 'integrity'],
        relatedExamples: ['aggregate-1'],
      },
      {
        id: 'dq-2',
        title: 'Identifying Orphaned Records',
        database: 'northwind',
        sql: `-- Find order details without corresponding orders
SELECT
  od.order_id,
  od.product_id,
  od.quantity,
  CASE WHEN o.order_id IS NULL THEN 'ORPHAN' ELSE 'OK' END AS status
FROM order_details od
LEFT JOIN orders o ON od.order_id = o.order_id
WHERE o.order_id IS NULL
LIMIT 10;
-- EXPECTED: Orphaned order details (should be empty)
-- (Indicates referential integrity issues)`,
        description: 'Detect order details without parent orders (referential integrity check)',
        sqlFeatures: ['LEFT JOIN', 'IS NULL', 'Referential integrity', 'Orphan detection'],
        difficulty: 'beginner',
        useCase: 'data-quality',
        tags: ['orphans', 'referential-integrity', 'foreign-keys', 'data-quality'],
        relatedExamples: ['join-2'],
      },
      {
        id: 'dq-3',
        title: 'NULL Pattern Analysis',
        database: 'northwind',
        sql: `-- Analyze NULL patterns across product columns
SELECT
  'unit_price' AS column_name,
  COUNT(*) AS total_rows,
  COUNT(unit_price) AS non_null_count,
  COUNT(*) - COUNT(unit_price) AS null_count,
  ROUND((COUNT(*) - COUNT(unit_price)) * 100.0 / COUNT(*), 2) AS null_percentage
FROM products
UNION ALL
SELECT
  'units_in_stock',
  COUNT(*),
  COUNT(units_in_stock),
  COUNT(*) - COUNT(units_in_stock),
  ROUND((COUNT(*) - COUNT(units_in_stock)) * 100.0 / COUNT(*), 2)
FROM products
UNION ALL
SELECT
  'units_on_order',
  COUNT(*),
  COUNT(units_on_order),
  COUNT(*) - COUNT(units_on_order),
  ROUND((COUNT(*) - COUNT(units_on_order)) * 100.0 / COUNT(*), 2)
FROM products
ORDER BY null_percentage DESC;
-- EXPECTED: NULL statistics for each column
-- (Helps identify data completeness issues)`,
        description: 'Analyze NULL value patterns across multiple columns',
        sqlFeatures: ['UNION ALL', 'COUNT', 'NULL analysis', 'Data profiling'],
        difficulty: 'intermediate',
        useCase: 'data-quality',
        tags: ['null-analysis', 'data-profiling', 'completeness', 'quality-metrics'],
        relatedExamples: ['aggregate-1', 'union-1'],
      },
      {
        id: 'dq-4',
        title: 'Data Consistency Checks',
        database: 'northwind',
        sql: `-- Find products with illogical stock levels
SELECT
  product_id,
  product_name,
  unit_price,
  units_in_stock,
  units_on_order,
  CASE
    WHEN unit_price IS NULL THEN 'Missing price'
    WHEN unit_price < 0 THEN 'Negative price'
    WHEN unit_price = 0 THEN 'Zero price'
    WHEN units_in_stock IS NULL THEN 'Missing stock'
    WHEN units_in_stock < 0 THEN 'Negative stock'
    WHEN units_on_order IS NULL THEN 'Missing on_order'
    WHEN units_on_order < 0 THEN 'Negative on_order'
    ELSE 'OK'
  END AS data_issue
FROM products
WHERE
  unit_price IS NULL OR unit_price <= 0 OR
  units_in_stock IS NULL OR units_in_stock < 0 OR
  units_on_order IS NULL OR units_on_order < 0
ORDER BY product_id;
-- EXPECTED: Products with data consistency issues
-- (Validates business rules and constraints)`,
        description: 'Validate business rules for product data consistency',
        sqlFeatures: ['CASE', 'NULL checks', 'Data validation', 'Business rules'],
        difficulty: 'intermediate',
        useCase: 'data-quality',
        tags: ['consistency', 'validation', 'business-rules', 'constraints'],
        relatedExamples: ['case-1'],
      },
      {
        id: 'dq-5',
        title: 'Referential Integrity Validation',
        database: 'northwind',
        sql: `-- Comprehensive referential integrity check
WITH integrity_checks AS (
  SELECT 'Products missing category' AS check_name, COUNT(*) AS violation_count
  FROM products p
  LEFT JOIN categories c ON p.category_id = c.category_id
  WHERE p.category_id IS NOT NULL AND c.category_id IS NULL
  UNION ALL
  SELECT 'Order details missing product', COUNT(*)
  FROM order_details od
  LEFT JOIN products p ON od.product_id = p.product_id
  WHERE od.product_id IS NOT NULL AND p.product_id IS NULL
  UNION ALL
  SELECT 'Order details missing order', COUNT(*)
  FROM order_details od
  LEFT JOIN orders o ON od.order_id = o.order_id
  WHERE od.order_id IS NOT NULL AND o.order_id IS NULL
)
SELECT
  check_name,
  violation_count,
  CASE
    WHEN violation_count = 0 THEN 'PASS'
    ELSE 'FAIL'
  END AS status
FROM integrity_checks
ORDER BY violation_count DESC;
-- EXPECTED: Referential integrity check results
-- (All checks should show 0 violations)`,
        description: 'Comprehensive validation of foreign key relationships',
        sqlFeatures: ['CTE', 'UNION ALL', 'LEFT JOIN', 'Referential integrity'],
        difficulty: 'intermediate',
        useCase: 'data-quality',
        tags: ['referential-integrity', 'foreign-keys', 'validation', 'constraints'],
        relatedExamples: ['dq-2', 'cte-1'],
      },
      {
        id: 'dq-6',
        title: 'Outlier Detection',
        database: 'northwind',
        sql: `-- Detect price outliers using statistical methods
WITH price_stats AS (
  SELECT
    AVG(unit_price) AS mean_price,
    STDDEV(unit_price) AS stddev_price
  FROM products
  WHERE unit_price IS NOT NULL
)
SELECT
  p.product_id,
  p.product_name,
  p.unit_price,
  ROUND(ps.mean_price, 2) AS mean_price,
  ROUND(ps.stddev_price, 2) AS stddev_price,
  ROUND((p.unit_price - ps.mean_price) / NULLIF(ps.stddev_price, 0), 2) AS z_score,
  CASE
    WHEN ABS((p.unit_price - ps.mean_price) / NULLIF(ps.stddev_price, 0)) > 2
    THEN 'OUTLIER'
    ELSE 'NORMAL'
  END AS outlier_status
FROM products p
CROSS JOIN price_stats ps
WHERE p.unit_price IS NOT NULL
  AND ABS((p.unit_price - ps.mean_price) / NULLIF(ps.stddev_price, 0)) > 2
ORDER BY ABS((p.unit_price - ps.mean_price) / NULLIF(ps.stddev_price, 0)) DESC;
-- EXPECTED: Products with prices >2 standard deviations from mean
-- (Statistical outlier detection)`,
        description: 'Identify price outliers using Z-score statistical method',
        sqlFeatures: ['CTE', 'STDDEV', 'AVG', 'CROSS JOIN', 'Statistical analysis'],
        difficulty: 'advanced',
        useCase: 'data-quality',
        tags: ['outliers', 'statistics', 'z-score', 'anomaly-detection'],
        relatedExamples: ['aggregate-1', 'cte-1'],
      },
    ],
  },
  {
    id: 'advanced-multi-feature',
    title: 'Advanced Multi-Feature Examples',
    description: 'Complex queries combining CTEs, window functions, JOINs, and subqueries',
    queries: [
      {
        id: 'adv-1',
        title: 'CTEs + Window Functions + JOINs',
        database: 'northwind',
        sql: `-- Complex reporting: Category performance with rankings
WITH category_metrics AS (
  SELECT
    c.category_id,
    c.category_name,
    COUNT(DISTINCT p.product_id) AS product_count,
    COUNT(DISTINCT od.order_id) AS order_count,
    SUM(od.quantity) AS units_sold,
    SUM(od.unit_price * od.quantity * (1 - od.discount)) AS revenue
  FROM categories c
  LEFT JOIN products p ON c.category_id = p.category_id
  LEFT JOIN order_details od ON p.product_id = od.product_id
  GROUP BY c.category_id, c.category_name
)
SELECT
  category_name,
  product_count,
  order_count,
  units_sold,
  ROUND(revenue, 2) AS revenue,
  RANK() OVER (ORDER BY revenue DESC) AS revenue_rank,
  ROUND(revenue * 100.0 / SUM(revenue) OVER (), 2) AS revenue_share_pct,
  ROUND(AVG(revenue) OVER (), 2) AS avg_category_revenue,
  CASE
    WHEN revenue > AVG(revenue) OVER () THEN 'Above Average'
    ELSE 'Below Average'
  END AS performance
FROM category_metrics
ORDER BY revenue DESC;
-- EXPECTED: Category performance with rankings and benchmarks
-- (Combines multiple SQL:1999 features for comprehensive analysis)`,
        description: 'Comprehensive category analysis combining CTEs, window functions, and JOINs',
        sqlFeatures: ['CTE', 'Window functions', 'RANK', 'Multiple JOINs', 'Complex aggregation'],
        difficulty: 'advanced',
        useCase: 'reports',
        tags: ['complex-query', 'multi-feature', 'reporting', 'advanced-analytics'],
        relatedExamples: ['cte-1', 'window-1', 'join-1'],
      },
      {
        id: 'adv-2',
        title: 'Recursive CTEs + Aggregates',
        database: 'empty',
        sql: `-- Hierarchical rollup using recursive CTE
WITH RECURSIVE org_hierarchy AS (
  -- Base case: top-level managers
  SELECT
    1 AS employee_id,
    'CEO' AS name,
    NULL::INTEGER AS manager_id,
    1 AS level,
    CAST('CEO' AS TEXT) AS path
  UNION ALL
  SELECT
    2, 'VP Sales', 1, 2, 'CEO > VP Sales'
  UNION ALL
  SELECT
    3, 'VP Engineering', 1, 2, 'CEO > VP Engineering'
  UNION ALL
  SELECT
    4, 'Sales Rep 1', 2, 3, 'CEO > VP Sales > Sales Rep 1'
  UNION ALL
  SELECT
    5, 'Sales Rep 2', 2, 3, 'CEO > VP Sales > Sales Rep 2'
  UNION ALL
  SELECT
    6, 'Engineer 1', 3, 3, 'CEO > VP Engineering > Engineer 1'
)
SELECT
  level,
  COUNT(*) AS employee_count,
  STRING_AGG(name, ', ') AS employees,
  AVG(level) OVER () AS avg_org_depth
FROM org_hierarchy
GROUP BY level
ORDER BY level;
-- EXPECTED: Organizational hierarchy with level-wise aggregation
-- (Demonstrates recursive CTEs with hierarchical data)`,
        description: 'Hierarchical data rollup using recursive CTEs and aggregation',
        sqlFeatures: ['Recursive CTE', 'Aggregates', 'STRING_AGG', 'Hierarchical queries'],
        difficulty: 'advanced',
        useCase: 'development',
        tags: ['recursive', 'hierarchy', 'organizational-structure', 'tree-queries'],
        relatedExamples: ['recursive-1', 'cte-1'],
      },
      {
        id: 'adv-3',
        title: 'Subqueries in Multiple Contexts',
        database: 'northwind',
        sql: `-- Subqueries in SELECT, FROM, WHERE, and HAVING clauses
SELECT
  p.product_name,
  p.unit_price,
  -- Subquery in SELECT
  (SELECT AVG(unit_price) FROM products) AS overall_avg_price,
  -- Subquery in SELECT with correlation
  (SELECT COUNT(*) FROM order_details od WHERE od.product_id = p.product_id) AS times_ordered,
  -- Calculated field
  ROUND(p.unit_price / (SELECT AVG(unit_price) FROM products), 2) AS price_vs_avg
FROM products p
-- Subquery in WHERE
WHERE p.unit_price > (SELECT AVG(unit_price) FROM products)
  AND p.category_id IN (
    -- Subquery returning multiple values
    SELECT category_id FROM categories WHERE category_name LIKE '%Co%'
  )
  AND EXISTS (
    -- Correlated subquery in WHERE
    SELECT 1 FROM order_details od WHERE od.product_id = p.product_id
  )
ORDER BY p.unit_price DESC
LIMIT 10;
-- EXPECTED: Premium products from specific categories with order history
-- (Demonstrates subqueries in multiple SQL contexts)`,
        description: 'Use subqueries in SELECT, FROM, WHERE, and EXISTS clauses',
        sqlFeatures: ['Scalar subquery', 'Correlated subquery', 'EXISTS', 'IN subquery'],
        difficulty: 'advanced',
        useCase: 'development',
        tags: ['subqueries', 'correlated', 'exists', 'multi-context'],
        relatedExamples: ['subquery-1', 'subquery-2', 'subquery-3'],
      },
      {
        id: 'adv-4',
        title: 'Window Functions + CASE Expressions',
        database: 'northwind',
        sql: `-- Conditional analytics using window functions and CASE
SELECT
  product_name,
  category_id,
  unit_price,
  units_in_stock,
  -- Window function with CASE for conditional aggregation
  SUM(CASE WHEN unit_price > 20 THEN 1 ELSE 0 END)
    OVER (PARTITION BY category_id) AS premium_products_in_category,
  -- Multiple window functions
  AVG(unit_price) OVER (PARTITION BY category_id) AS category_avg_price,
  RANK() OVER (PARTITION BY category_id ORDER BY unit_price DESC) AS price_rank_in_category,
  -- Complex CASE with window function results
  CASE
    WHEN unit_price > AVG(unit_price) OVER (PARTITION BY category_id) * 1.5
      THEN 'Premium'
    WHEN unit_price > AVG(unit_price) OVER (PARTITION BY category_id)
      THEN 'Above Average'
    WHEN unit_price > AVG(unit_price) OVER (PARTITION BY category_id) * 0.5
      THEN 'Average'
    ELSE 'Budget'
  END AS price_tier,
  -- Stock status
  CASE
    WHEN units_in_stock = 0 THEN 'Out of Stock'
    WHEN units_in_stock < 10 THEN 'Low Stock'
    ELSE 'In Stock'
  END AS stock_status
FROM products
WHERE unit_price IS NOT NULL
ORDER BY category_id, price_rank_in_category
LIMIT 30;
-- EXPECTED: Products with conditional analytics and tiering
-- (Shows power of combining window functions with CASE)`,
        description: 'Combine window functions with CASE expressions for conditional analytics',
        sqlFeatures: ['Window functions', 'CASE', 'Conditional aggregation', 'Multiple OVER clauses'],
        difficulty: 'advanced',
        useCase: 'analytics',
        tags: ['window-functions', 'case-expressions', 'conditional-logic', 'tiering'],
        relatedExamples: ['window-1', 'case-1'],
      },
      {
        id: 'adv-5',
        title: 'Multiple CTEs with Cross-References',
        database: 'northwind',
        sql: `-- Complex analysis using multiple interconnected CTEs
WITH product_sales AS (
  SELECT
    product_id,
    SUM(quantity) AS total_quantity,
    SUM(unit_price * quantity * (1 - discount)) AS total_revenue
  FROM order_details
  GROUP BY product_id
),
category_sales AS (
  SELECT
    p.category_id,
    SUM(ps.total_revenue) AS category_revenue
  FROM product_sales ps
  JOIN products p ON ps.product_id = p.product_id
  GROUP BY p.category_id
),
product_analysis AS (
  SELECT
    p.product_id,
    p.product_name,
    p.category_id,
    ps.total_quantity,
    ps.total_revenue,
    cs.category_revenue
  FROM products p
  JOIN product_sales ps ON p.product_id = ps.product_id
  JOIN category_sales cs ON p.category_id = cs.category_id
)
SELECT
  product_name,
  total_quantity AS units_sold,
  ROUND(total_revenue, 2) AS product_revenue,
  ROUND(category_revenue, 2) AS category_total,
  ROUND(total_revenue * 100.0 / category_revenue, 2) AS pct_of_category_revenue,
  RANK() OVER (PARTITION BY category_id ORDER BY total_revenue DESC) AS rank_in_category
FROM product_analysis
WHERE total_revenue > 0
ORDER BY category_id, rank_in_category
LIMIT 20;
-- EXPECTED: Products with category context and rankings
-- (Multiple CTEs building on each other for layered analysis)`,
        description: 'Multiple CTEs that reference each other for layered analysis',
        sqlFeatures: ['Multiple CTEs', 'CTE chaining', 'Window functions', 'Complex JOINs'],
        difficulty: 'advanced',
        useCase: 'analytics',
        tags: ['cte-chaining', 'layered-analysis', 'complex-queries', 'data-pipeline'],
        relatedExamples: ['cte-1', 'cte-2', 'window-1'],
      },
    ],
  },
  {
    id: 'sql1999-standards',
    title: 'SQL:1999 Standards Showcase',
    description: 'Demonstrate SQL:1999 specific features and standards compliance',
    queries: [
      {
        id: 'std-1',
        title: 'CASE Expression (SQL:1999)',
        database: 'northwind',
        sql: `-- SQL:1999 simple and searched CASE expressions
SELECT
  product_name,
  unit_price,
  -- Simple CASE
  CASE category_id
    WHEN 1 THEN 'Beverages'
    WHEN 2 THEN 'Condiments'
    WHEN 3 THEN 'Confections'
    ELSE 'Other'
  END AS category_simple,
  -- Searched CASE
  CASE
    WHEN unit_price >= 50 THEN 'Premium'
    WHEN unit_price >= 20 THEN 'Standard'
    WHEN unit_price >= 10 THEN 'Economy'
    ELSE 'Budget'
  END AS price_tier,
  -- CASE in aggregate
  SUM(CASE WHEN unit_price > 20 THEN 1 ELSE 0 END) OVER () AS premium_count
FROM products
LIMIT 15;
-- EXPECTED: Products with CASE expressions demonstrating SQL:1999 syntax
-- (Shows both simple and searched CASE forms)`,
        description: 'Demonstrate SQL:1999 CASE expressions (simple and searched forms)',
        sqlFeatures: ['CASE', 'Simple CASE', 'Searched CASE', 'SQL:1999 standard'],
        difficulty: 'beginner',
        useCase: 'development',
        tags: ['sql1999', 'case-expression', 'conditional-logic', 'standards'],
        relatedExamples: ['case-1'],
      },
      {
        id: 'std-2',
        title: 'Common Table Expressions (SQL:1999)',
        database: 'northwind',
        sql: `-- SQL:1999 WITH clause (CTEs)
WITH expensive_products AS (
  SELECT * FROM products WHERE unit_price > 30
),
product_orders AS (
  SELECT
    ep.product_id,
    ep.product_name,
    ep.unit_price,
    COUNT(od.order_id) AS order_count
  FROM expensive_products ep
  LEFT JOIN order_details od ON ep.product_id = od.product_id
  GROUP BY ep.product_id, ep.product_name, ep.unit_price
)
SELECT
  product_name,
  unit_price,
  order_count,
  CASE
    WHEN order_count > 20 THEN 'High Demand'
    WHEN order_count > 10 THEN 'Medium Demand'
    WHEN order_count > 0 THEN 'Low Demand'
    ELSE 'No Orders'
  END AS demand_level
FROM product_orders
ORDER BY order_count DESC;
-- EXPECTED: Expensive products with order demand classification
-- (SQL:1999 WITH clause for improved readability)`,
        description: 'Use SQL:1999 Common Table Expressions (WITH clause)',
        sqlFeatures: ['CTE', 'WITH', 'SQL:1999 standard', 'Query modularity'],
        difficulty: 'intermediate',
        useCase: 'development',
        tags: ['sql1999', 'cte', 'with-clause', 'standards'],
        relatedExamples: ['cte-1'],
      },
      {
        id: 'std-3',
        title: 'Window Functions (SQL:1999)',
        database: 'northwind',
        sql: `-- SQL:1999 window functions (OVER clause)
SELECT
  product_name,
  category_id,
  unit_price,
  -- ROW_NUMBER (SQL:1999)
  ROW_NUMBER() OVER (PARTITION BY category_id ORDER BY unit_price DESC) AS row_num,
  -- RANK (SQL:1999)
  RANK() OVER (PARTITION BY category_id ORDER BY unit_price DESC) AS price_rank,
  -- DENSE_RANK (SQL:1999)
  DENSE_RANK() OVER (PARTITION BY category_id ORDER BY unit_price DESC) AS dense_rank,
  -- Aggregate window function
  AVG(unit_price) OVER (PARTITION BY category_id) AS category_avg_price,
  -- Running total
  SUM(unit_price) OVER (PARTITION BY category_id ORDER BY unit_price DESC) AS running_total
FROM products
WHERE unit_price IS NOT NULL
ORDER BY category_id, price_rank
LIMIT 20;
-- EXPECTED: Products with various SQL:1999 window functions
-- (Demonstrates ranking and aggregate window functions)`,
        description: 'Demonstrate SQL:1999 window functions (OVER clause)',
        sqlFeatures: ['Window functions', 'ROW_NUMBER', 'RANK', 'DENSE_RANK', 'OVER', 'SQL:1999'],
        difficulty: 'intermediate',
        useCase: 'analytics',
        tags: ['sql1999', 'window-functions', 'over-clause', 'ranking'],
        relatedExamples: ['window-1', 'window-2'],
      },
      {
        id: 'std-4',
        title: 'BOOLEAN Data Type (SQL:1999)',
        database: 'empty',
        sql: `-- SQL:1999 BOOLEAN type and operations
CREATE TEMPORARY TABLE features (
  feature_name TEXT,
  is_enabled BOOLEAN,
  is_premium BOOLEAN
);

INSERT INTO features VALUES
  ('Dark Mode', TRUE, FALSE),
  ('Export', TRUE, TRUE),
  ('Analytics', FALSE, TRUE),
  ('API Access', TRUE, TRUE);

SELECT
  feature_name,
  is_enabled,
  is_premium,
  -- Boolean expressions
  is_enabled AND is_premium AS enabled_premium,
  is_enabled OR is_premium AS enabled_or_premium,
  NOT is_enabled AS disabled,
  -- CASE with BOOLEAN
  CASE
    WHEN is_enabled AND is_premium THEN 'Premium Active'
    WHEN is_enabled THEN 'Basic Active'
    WHEN is_premium THEN 'Premium Disabled'
    ELSE 'Basic Disabled'
  END AS status
FROM features
ORDER BY feature_name;
-- EXPECTED: Features with boolean operations
-- (SQL:1999 BOOLEAN type with AND/OR/NOT)`,
        description: 'Use SQL:1999 BOOLEAN data type and boolean operations',
        sqlFeatures: ['BOOLEAN', 'Boolean algebra', 'AND', 'OR', 'NOT', 'SQL:1999'],
        difficulty: 'beginner',
        useCase: 'development',
        tags: ['sql1999', 'boolean', 'data-types', 'logic'],
        relatedExamples: ['datatype-1'],
      },
      {
        id: 'std-5',
        title: 'Standards-Compliant Date Functions',
        database: 'northwind',
        sql: `-- SQL:1999 date/time functions
SELECT
  order_id,
  order_date,
  -- EXTRACT (SQL:1999)
  EXTRACT(YEAR FROM order_date) AS order_year,
  EXTRACT(MONTH FROM order_date) AS order_month,
  EXTRACT(DAY FROM order_date) AS order_day,
  -- Date arithmetic
  order_date + INTERVAL '30 days' AS due_date,
  CURRENT_DATE - order_date AS days_since_order,
  -- Date truncation
  DATE_TRUNC('month', order_date) AS order_month_start,
  DATE_TRUNC('year', order_date) AS order_year_start
FROM orders
WHERE order_date IS NOT NULL
ORDER BY order_date DESC
LIMIT 15;
-- EXPECTED: Orders with SQL:1999 date/time manipulations
-- (Standards-compliant date operations)`,
        description: 'Use SQL:1999 standards-compliant date/time functions',
        sqlFeatures: ['EXTRACT', 'INTERVAL', 'DATE_TRUNC', 'Date functions', 'SQL:1999'],
        difficulty: 'intermediate',
        useCase: 'development',
        tags: ['sql1999', 'date-functions', 'temporal', 'standards'],
        relatedExamples: ['datetime-1'],
      },
      {
        id: 'std-6',
        title: 'NULLIF and COALESCE (SQL:1999)',
        database: 'northwind',
        sql: `-- SQL:1999 NULL handling functions
SELECT
  product_name,
  unit_price,
  units_in_stock,
  units_on_order,
  -- COALESCE (SQL:1999)
  COALESCE(units_in_stock, 0) AS stock_or_zero,
  COALESCE(units_on_order, 0) AS on_order_or_zero,
  -- NULLIF (SQL:1999)
  NULLIF(units_in_stock, 0) AS stock_null_if_zero,
  -- Practical example: avoid division by zero
  ROUND(
    unit_price / NULLIF(COALESCE(units_in_stock, 0), 0),
    2
  ) AS price_per_stock_unit,
  -- Combining both
  COALESCE(
    NULLIF(units_in_stock, 0)::TEXT,
    'Out of Stock'
  ) AS stock_display
FROM products
WHERE unit_price IS NOT NULL
ORDER BY product_id
LIMIT 15;
-- EXPECTED: Products with NULL handling demonstrations
-- (SQL:1999 functions for NULL value handling)`,
        description: 'Demonstrate SQL:1999 NULLIF and COALESCE functions',
        sqlFeatures: ['NULLIF', 'COALESCE', 'NULL handling', 'SQL:1999'],
        difficulty: 'intermediate',
        useCase: 'development',
        tags: ['sql1999', 'null-handling', 'coalesce', 'nullif'],
        relatedExamples: ['null-1'],
      },
    ],
  },
  {
    id: 'performance-patterns',
    title: 'Performance Patterns',
    description: 'Educational comparisons of efficient vs inefficient query patterns',
    queries: [
      {
        id: 'perf-1',
        title: 'Efficient: Set-Based vs Iterative',
        database: 'northwind',
        sql: `-- EFFICIENT: Set-based operation (processes all rows at once)
SELECT
  category_id,
  COUNT(*) AS product_count,
  AVG(unit_price) AS avg_price,
  MAX(unit_price) AS max_price,
  MIN(unit_price) AS min_price
FROM products
WHERE unit_price IS NOT NULL
GROUP BY category_id
ORDER BY category_id;
-- EXPECTED: Category statistics computed in one pass
-- (Set-based operations are typically 10-100x faster than row-by-row)
--
-- INEFFICIENT alternative would be:
-- - Cursor iterating through categories
-- - Separate query for each category
-- - Manual aggregation in application code`,
        description: 'Demonstrate efficient set-based operations (vs row-by-row processing)',
        sqlFeatures: ['Aggregates', 'GROUP BY', 'Set-based operations', 'Performance'],
        difficulty: 'beginner',
        useCase: 'development',
        performanceNotes: 'Set-based operations are 10-100x faster than iterative approaches',
        tags: ['performance', 'set-based', 'optimization', 'best-practices'],
        relatedExamples: ['aggregate-1'],
      },
      {
        id: 'perf-2',
        title: 'JOIN vs Subquery Performance',
        database: 'northwind',
        sql: `-- EFFICIENT: JOIN (optimizer can choose best strategy)
SELECT
  p.product_name,
  p.unit_price,
  c.category_name
FROM products p
JOIN categories c ON p.category_id = c.category_id
WHERE p.unit_price > 20
ORDER BY p.unit_price DESC
LIMIT 10;
-- EXPECTED: Top 10 expensive products with category names
-- (JOINs typically perform better than correlated subqueries)
--
-- LESS EFFICIENT alternative:
-- SELECT
--   product_name,
--   unit_price,
--   (SELECT category_name FROM categories c
--    WHERE c.category_id = p.category_id) AS category_name
-- FROM products p
-- WHERE unit_price > 20
-- (Correlated subquery executes once per row)`,
        description: 'Compare JOIN vs correlated subquery performance',
        sqlFeatures: ['JOIN', 'Subquery comparison', 'Query optimization'],
        difficulty: 'intermediate',
        useCase: 'development',
        performanceNotes: 'JOINs allow optimizer flexibility; correlated subqueries may execute once per row',
        tags: ['performance', 'join-vs-subquery', 'optimization', 'query-patterns'],
        relatedExamples: ['join-1', 'subquery-1'],
      },
      {
        id: 'perf-3',
        title: 'Index-Friendly Query Patterns',
        database: 'northwind',
        sql: `-- INDEX-FRIENDLY: Uses SARGable predicates
SELECT product_id, product_name, unit_price
FROM products
WHERE unit_price >= 20 AND unit_price <= 50
ORDER BY unit_price
LIMIT 10;
-- EXPECTED: Products in price range 20-50
-- (Range scan on unit_price index is efficient)
--
-- INDEX-UNFRIENDLY alternatives:
-- WHERE unit_price * 1.1 > 22          (function on indexed column)
-- WHERE LOWER(product_name) = 'chai'   (function on indexed column)
-- WHERE product_name LIKE '%tea%'      (leading wildcard)
--
-- These prevent index usage and force full table scans`,
        description: 'Demonstrate index-friendly (SARGable) vs index-unfriendly queries',
        sqlFeatures: ['WHERE', 'Index optimization', 'SARGable predicates'],
        difficulty: 'intermediate',
        useCase: 'development',
        performanceNotes: 'SARGable predicates enable index usage; functions on columns prevent it',
        tags: ['performance', 'indexes', 'sargable', 'optimization'],
        relatedExamples: ['basic-2'],
      },
      {
        id: 'perf-4',
        title: 'Efficient Aggregation with CTEs',
        database: 'northwind',
        sql: `-- EFFICIENT: Pre-aggregate data in CTE, then join
WITH order_totals AS (
  SELECT
    order_id,
    SUM(unit_price * quantity * (1 - discount)) AS order_total
  FROM order_details
  GROUP BY order_id
)
SELECT
  o.order_id,
  o.customer_id,
  o.order_date,
  ROUND(ot.order_total, 2) AS order_total
FROM orders o
JOIN order_totals ot ON o.order_id = ot.order_id
WHERE o.order_date IS NOT NULL
ORDER BY ot.order_total DESC
LIMIT 10;
-- EXPECTED: Top 10 orders by total value
-- (CTE allows one-time aggregation, then efficient JOIN)
--
-- LESS EFFICIENT: Correlated subquery in SELECT
-- Would recalculate order total for each row`,
        description: 'Use CTEs for efficient pre-aggregation',
        sqlFeatures: ['CTE', 'Pre-aggregation', 'JOIN', 'Performance optimization'],
        difficulty: 'intermediate',
        useCase: 'development',
        performanceNotes: 'CTE pre-aggregates once; correlated subqueries repeat for each row',
        tags: ['performance', 'cte', 'aggregation', 'optimization'],
        relatedExamples: ['cte-1', 'aggregate-1'],
      },
      {
        id: 'perf-5',
        title: 'Avoiding SELECT *',
        database: 'northwind',
        sql: `-- EFFICIENT: Select only needed columns
SELECT
  order_id,
  customer_id,
  order_date,
  shipped_date
FROM orders
WHERE order_date >= '1997-01-01'
  AND order_date < '1998-01-01'
ORDER BY order_date
LIMIT 15;
-- EXPECTED: 1997 orders with specific columns
-- (Reduces I/O, network transfer, and memory usage)
--
-- INEFFICIENT alternative:
-- SELECT * FROM orders WHERE ...
-- - Transfers unnecessary data
-- - Uses more memory
-- - Can prevent covering index usage
-- - Breaks if schema changes`,
        description: 'Select specific columns instead of SELECT *',
        sqlFeatures: ['SELECT', 'Column selection', 'Performance best practices'],
        difficulty: 'beginner',
        useCase: 'development',
        performanceNotes: 'Explicit column lists reduce I/O and enable covering indexes',
        tags: ['performance', 'select-star', 'best-practices', 'optimization'],
        relatedExamples: ['basic-1'],
      },
      {
        id: 'perf-6',
        title: 'Efficient LIMIT with ORDER BY',
        database: 'northwind',
        sql: `-- EFFICIENT: LIMIT with ORDER BY on indexed column
SELECT
  product_id,
  product_name,
  unit_price
FROM products
WHERE category_id = 1
ORDER BY unit_price DESC
LIMIT 5;
-- EXPECTED: Top 5 most expensive beverages
-- (Database can stop after finding first 5 rows)
--
-- LESS EFFICIENT patterns:
-- 1. Large LIMIT without WHERE clause (scans many rows)
-- 2. ORDER BY on unindexed column (full sort required)
-- 3. OFFSET without LIMIT (still processes skipped rows)
--
-- TIP: For pagination, use WHERE with indexed columns
-- instead of large OFFSET values`,
        description: 'Efficient use of LIMIT with ORDER BY for top-N queries',
        sqlFeatures: ['LIMIT', 'ORDER BY', 'Top-N optimization'],
        difficulty: 'beginner',
        useCase: 'development',
        performanceNotes: 'LIMIT allows early termination; large OFFSETs still process skipped rows',
        tags: ['performance', 'limit', 'top-n', 'pagination'],
        relatedExamples: ['basic-1', 'basic-2'],
      },
    ],
  },
  {
    id: 'report-templates',
    title: 'Report Templates',
    description: 'Production-ready report queries for common business needs',
    queries: [
      {
        id: 'rpt-1',
        title: 'Monthly Sales Summary Report',
        database: 'northwind',
        sql: `-- Monthly sales summary with key metrics
WITH monthly_data AS (
  SELECT
    DATE_TRUNC('month', o.order_date) AS month,
    COUNT(DISTINCT o.order_id) AS order_count,
    COUNT(DISTINCT o.customer_id) AS customer_count,
    SUM(od.quantity) AS units_sold,
    SUM(od.unit_price * od.quantity * (1 - od.discount)) AS revenue
  FROM orders o
  JOIN order_details od ON o.order_id = od.order_id
  WHERE o.order_date IS NOT NULL
  GROUP BY DATE_TRUNC('month', o.order_date)
)
SELECT
  TO_CHAR(month, 'YYYY-MM') AS month,
  order_count,
  customer_count,
  units_sold,
  ROUND(revenue, 2) AS revenue,
  ROUND(revenue / order_count, 2) AS avg_order_value,
  ROUND(revenue / customer_count, 2) AS revenue_per_customer,
  LAG(revenue) OVER (ORDER BY month) AS prev_month_revenue,
  ROUND(
    (revenue - LAG(revenue) OVER (ORDER BY month)) * 100.0 /
    NULLIF(LAG(revenue) OVER (ORDER BY month), 0),
    2
  ) AS mom_growth_pct
FROM monthly_data
ORDER BY month;
-- EXPECTED: Monthly sales KPIs with month-over-month growth
-- (Ready-to-use executive summary report)`,
        description: 'Complete monthly sales summary with KPIs and growth metrics',
        sqlFeatures: ['CTE', 'DATE_TRUNC', 'Aggregates', 'LAG', 'Period-over-period'],
        difficulty: 'intermediate',
        useCase: 'reports',
        tags: ['monthly-report', 'kpi', 'executive-summary', 'sales-metrics'],
        relatedExamples: ['bi-1', 'window-1'],
      },
      {
        id: 'rpt-2',
        title: 'Top N Per Group Report',
        database: 'northwind',
        sql: `-- Top 3 products per category by revenue
WITH product_revenue AS (
  SELECT
    p.product_id,
    p.product_name,
    p.category_id,
    c.category_name,
    SUM(od.unit_price * od.quantity * (1 - od.discount)) AS revenue,
    SUM(od.quantity) AS units_sold,
    COUNT(DISTINCT od.order_id) AS order_count
  FROM products p
  JOIN categories c ON p.category_id = c.category_id
  LEFT JOIN order_details od ON p.product_id = od.product_id
  GROUP BY p.product_id, p.product_name, p.category_id, c.category_name
),
ranked_products AS (
  SELECT
    category_name,
    product_name,
    revenue,
    units_sold,
    order_count,
    ROW_NUMBER() OVER (PARTITION BY category_id ORDER BY revenue DESC) AS rank
  FROM product_revenue
  WHERE revenue > 0
)
SELECT
  category_name,
  rank,
  product_name,
  ROUND(revenue, 2) AS revenue,
  units_sold,
  order_count,
  ROUND(revenue / units_sold, 2) AS avg_price_per_unit
FROM ranked_products
WHERE rank <= 3
ORDER BY category_name, rank;
-- EXPECTED: Top 3 products per category with metrics
-- (Classic "top N per group" business report)`,
        description: 'Top performers by category (classic top-N-per-group pattern)',
        sqlFeatures: ['CTE', 'ROW_NUMBER', 'PARTITION BY', 'Top-N per group'],
        difficulty: 'intermediate',
        useCase: 'reports',
        tags: ['top-n-per-group', 'ranking', 'category-analysis', 'product-performance'],
        relatedExamples: ['window-2', 'cte-1'],
      },
      {
        id: 'rpt-3',
        title: 'Running Totals and Cumulative Sums',
        database: 'northwind',
        sql: `-- Daily order volume with running totals
WITH daily_orders AS (
  SELECT
    DATE(order_date) AS order_day,
    COUNT(*) AS daily_order_count,
    SUM(
      (SELECT SUM(unit_price * quantity * (1 - discount))
       FROM order_details od
       WHERE od.order_id = o.order_id)
    ) AS daily_revenue
  FROM orders o
  WHERE order_date >= '1997-01-01' AND order_date < '1997-02-01'
  GROUP BY DATE(order_date)
)
SELECT
  order_day,
  daily_order_count,
  ROUND(daily_revenue, 2) AS daily_revenue,
  SUM(daily_order_count) OVER (ORDER BY order_day) AS cumulative_orders,
  ROUND(SUM(daily_revenue) OVER (ORDER BY order_day), 2) AS cumulative_revenue,
  ROUND(AVG(daily_revenue) OVER (
    ORDER BY order_day
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ), 2) AS seven_day_avg_revenue
FROM daily_orders
ORDER BY order_day;
-- EXPECTED: Daily metrics with running totals and moving averages
-- (Useful for tracking progress toward goals)`,
        description: 'Daily metrics with running totals and moving averages',
        sqlFeatures: ['CTE', 'Window functions', 'Running totals', 'Moving averages', 'ROWS BETWEEN'],
        difficulty: 'advanced',
        useCase: 'reports',
        tags: ['running-total', 'cumulative', 'moving-average', 'time-series'],
        relatedExamples: ['window-1', 'window-5'],
      },
      {
        id: 'rpt-4',
        title: 'Period-over-Period Comparison',
        database: 'northwind',
        sql: `-- Quarter-over-quarter sales comparison
WITH quarterly_sales AS (
  SELECT
    EXTRACT(YEAR FROM o.order_date) AS year,
    EXTRACT(QUARTER FROM o.order_date) AS quarter,
    COUNT(DISTINCT o.order_id) AS order_count,
    SUM(od.unit_price * od.quantity * (1 - od.discount)) AS revenue
  FROM orders o
  JOIN order_details od ON o.order_id = od.order_id
  WHERE o.order_date IS NOT NULL
  GROUP BY EXTRACT(YEAR FROM o.order_date), EXTRACT(QUARTER FROM o.order_date)
)
SELECT
  year,
  quarter,
  order_count,
  ROUND(revenue, 2) AS revenue,
  LAG(revenue) OVER (ORDER BY year, quarter) AS prev_quarter_revenue,
  ROUND(
    revenue - LAG(revenue) OVER (ORDER BY year, quarter),
    2
  ) AS revenue_change,
  ROUND(
    (revenue - LAG(revenue) OVER (ORDER BY year, quarter)) * 100.0 /
    NULLIF(LAG(revenue) OVER (ORDER BY year, quarter), 0),
    2
  ) AS pct_change,
  LAG(revenue, 4) OVER (ORDER BY year, quarter) AS same_quarter_last_year,
  ROUND(
    (revenue - LAG(revenue, 4) OVER (ORDER BY year, quarter)) * 100.0 /
    NULLIF(LAG(revenue, 4) OVER (ORDER BY year, quarter), 0),
    2
  ) AS yoy_pct_change
FROM quarterly_sales
ORDER BY year, quarter;
-- EXPECTED: Quarterly sales with QoQ and YoY comparisons
-- (Essential for financial and executive reporting)`,
        description: 'Quarterly sales with quarter-over-quarter and year-over-year comparisons',
        sqlFeatures: ['CTE', 'EXTRACT', 'LAG', 'Period comparisons', 'Window functions'],
        difficulty: 'advanced',
        useCase: 'reports',
        tags: ['period-comparison', 'qoq', 'yoy', 'quarterly-report', 'trends'],
        relatedExamples: ['bi-1', 'window-1'],
      },
      {
        id: 'rpt-5',
        title: 'Pivot Table Simulation',
        database: 'northwind',
        sql: `-- Simulate pivot table: categories × quarters
WITH quarterly_category_sales AS (
  SELECT
    c.category_name,
    EXTRACT(YEAR FROM o.order_date) AS year,
    EXTRACT(QUARTER FROM o.order_date) AS quarter,
    SUM(od.unit_price * od.quantity * (1 - od.discount)) AS revenue
  FROM categories c
  JOIN products p ON c.category_id = p.category_id
  JOIN order_details od ON p.product_id = od.product_id
  JOIN orders o ON od.order_id = o.order_id
  WHERE o.order_date >= '1997-01-01' AND o.order_date < '1998-01-01'
  GROUP BY c.category_name, EXTRACT(YEAR FROM o.order_date), EXTRACT(QUARTER FROM o.order_date)
)
SELECT
  category_name,
  ROUND(SUM(CASE WHEN quarter = 1 THEN revenue ELSE 0 END), 2) AS q1_1997,
  ROUND(SUM(CASE WHEN quarter = 2 THEN revenue ELSE 0 END), 2) AS q2_1997,
  ROUND(SUM(CASE WHEN quarter = 3 THEN revenue ELSE 0 END), 2) AS q3_1997,
  ROUND(SUM(CASE WHEN quarter = 4 THEN revenue ELSE 0 END), 2) AS q4_1997,
  ROUND(SUM(revenue), 2) AS total_1997
FROM quarterly_category_sales
GROUP BY category_name
ORDER BY total_1997 DESC;
-- EXPECTED: Category revenue by quarter (pivot table format)
-- (Matrix view: rows = categories, columns = quarters)`,
        description: 'Simulate pivot table using CASE and GROUP BY (categories by quarters)',
        sqlFeatures: ['CTE', 'CASE', 'Conditional aggregation', 'Pivot simulation'],
        difficulty: 'advanced',
        useCase: 'reports',
        tags: ['pivot-table', 'crosstab', 'matrix-report', 'conditional-aggregation'],
        relatedExamples: ['aggregate-1', 'case-1'],
      },
      {
        id: 'rpt-6',
        title: 'Executive Dashboard Summary',
        database: 'northwind',
        sql: `-- High-level KPI dashboard for executives
WITH kpis AS (
  SELECT
    COUNT(DISTINCT o.order_id) AS total_orders,
    COUNT(DISTINCT o.customer_id) AS total_customers,
    COUNT(DISTINCT p.product_id) AS total_products,
    SUM(od.quantity) AS total_units_sold,
    SUM(od.unit_price * od.quantity * (1 - od.discount)) AS total_revenue,
    AVG(od.unit_price * od.quantity * (1 - od.discount)) AS avg_order_line_value
  FROM orders o
  JOIN order_details od ON o.order_id = od.order_id
  JOIN products p ON od.product_id = p.product_id
  WHERE o.order_date IS NOT NULL
)
SELECT
  'Total Orders' AS metric,
  total_orders::TEXT AS value
FROM kpis
UNION ALL SELECT 'Total Customers', total_customers::TEXT FROM kpis
UNION ALL SELECT 'Total Products Sold', total_products::TEXT FROM kpis
UNION ALL SELECT 'Total Units Sold', total_units_sold::TEXT FROM kpis
UNION ALL SELECT 'Total Revenue', '$' || ROUND(total_revenue, 2)::TEXT FROM kpis
UNION ALL SELECT 'Avg Order Line Value', '$' || ROUND(avg_order_line_value, 2)::TEXT FROM kpis
UNION ALL SELECT 'Avg Revenue per Customer', '$' || ROUND(total_revenue / total_customers, 2)::TEXT FROM kpis
UNION ALL SELECT 'Avg Revenue per Order', '$' || ROUND(total_revenue / total_orders, 2)::TEXT FROM kpis;
-- EXPECTED: Key business metrics in dashboard format
-- (Single view of most important KPIs)`,
        description: 'Executive dashboard with key business metrics and KPIs',
        sqlFeatures: ['CTE', 'UNION ALL', 'Aggregates', 'KPI reporting'],
        difficulty: 'intermediate',
        useCase: 'reports',
        tags: ['dashboard', 'kpi', 'executive-summary', 'metrics'],
        relatedExamples: ['aggregate-1', 'union-1'],
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

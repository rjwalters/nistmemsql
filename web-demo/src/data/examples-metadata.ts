/**
 * SQL:1999 Feature Showcase - Example Metadata
 *
 * Metadata for SQL examples organized by feature category.
 * SQL payloads and expected results are loaded separately from JSON files.
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

export interface ExampleMetadata {
  id: string
  title: string
  database: 'northwind' | 'employees' | 'company' | 'university' | 'empty'
  description: string
  sqlFeatures: string[]
  difficulty?: 'beginner' | 'intermediate' | 'advanced'
  useCase?: 'analytics' | 'admin' | 'development' | 'reports' | 'data-quality'
  performanceNotes?: string
  executionTimeMs?: number
  relatedExamples?: string[]
  tags?: string[]
}

export interface CategoryMetadata {
  id: string
  title: string
  description: string
  queries: ExampleMetadata[]
}

/**
 * Metadata for all example categories (without SQL payloads)
 */
export const exampleCategoriesMetadata: CategoryMetadata[] = [
  {
    id: 'basic',
    title: 'Basic Queries',
    description: 'SELECT, WHERE, ORDER BY, LIMIT fundamentals',
    queries: [
      {
        id: 'basic-1',
        title: 'Simple SELECT',
        database: 'northwind',
        description: 'Retrieve first 5 products',
        sqlFeatures: ['SELECT', 'LIMIT']
      },
      {
        id: 'basic-2',
        title: 'WHERE clause filtering',
        database: 'northwind',
        description: 'Filter products by price with sorting',
        sqlFeatures: ['SELECT', 'WHERE', 'ORDER BY']
      },
      {
        id: 'basic-3',
        title: 'Column aliases',
        database: 'northwind',
        description: 'Use aliases for columns and calculated fields',
        sqlFeatures: ['SELECT', 'AS', 'Expressions']
      },
      {
        id: 'basic-4',
        title: 'DISTINCT values',
        database: 'northwind',
        description: 'Get unique category IDs',
        sqlFeatures: ['SELECT', 'DISTINCT', 'ORDER BY']
      }
    ]
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
        description: 'Convert text to uppercase and lowercase',
        sqlFeatures: ['UPPER', 'LOWER']
      },
      {
        id: 'string-2',
        title: 'String Extraction',
        database: 'employees',
        description: 'Extract substrings and measure string length',
        sqlFeatures: ['SUBSTRING', 'LENGTH']
      },
      {
        id: 'string-3',
        title: 'Text Trimming',
        database: 'northwind',
        description: 'Remove leading and trailing whitespace',
        sqlFeatures: ['TRIM', 'LENGTH', '||']
      },
      {
        id: 'string-4',
        title: 'String Concatenation',
        database: 'employees',
        description: 'Combine strings using || operator and CONCAT function',
        sqlFeatures: ['CONCAT', '||']
      },
      {
        id: 'string-5',
        title: 'LEFT and RIGHT Extraction',
        database: 'northwind',
        description: 'Extract characters from start and end of strings',
        sqlFeatures: ['LEFT', 'RIGHT', 'LENGTH']
      },
      {
        id: 'string-6',
        title: 'String Replacement',
        database: 'northwind',
        description: 'Replace characters or substrings within text',
        sqlFeatures: ['REPLACE', 'LIKE']
      },
      {
        id: 'string-7',
        title: 'String Search',
        database: 'employees',
        description: 'Find position of substring within text',
        sqlFeatures: ['POSITION', 'LOWER']
      },
      {
        id: 'string-8',
        title: 'Text Reversal',
        database: 'northwind',
        description: 'Reverse string character order',
        sqlFeatures: ['REVERSE', 'UPPER']
      }
    ]
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
        description: 'Join products with their categories',
        sqlFeatures: ['INNER JOIN', 'ON', 'ORDER BY']
      },
      {
        id: 'join-2',
        title: 'LEFT OUTER JOIN',
        database: 'northwind',
        description: 'Show all categories with product counts (including empty categories)',
        sqlFeatures: ['LEFT OUTER JOIN', 'GROUP BY', 'COUNT']
      },
      {
        id: 'join-3',
        title: 'Multiple JOINs',
        database: 'employees',
        description: 'Self-join to show employees and their managers',
        sqlFeatures: ['INNER JOIN', 'Self-join', 'String concatenation']
      },
      {
        id: 'join-4',
        title: 'CROSS JOIN',
        database: 'northwind',
        description: 'Cartesian product of categories (all pairs)',
        sqlFeatures: ['CROSS JOIN', 'WHERE', 'LIMIT']
      }
    ]
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
        description: 'Aggregate statistics for all products',
        sqlFeatures: ['COUNT', 'AVG', 'MIN', 'MAX']
      },
      {
        id: 'agg-2',
        title: 'GROUP BY with COUNT',
        database: 'northwind',
        description: 'Count and average price of products by category',
        sqlFeatures: ['GROUP BY', 'COUNT', 'AVG', 'INNER JOIN']
      },
      {
        id: 'agg-3',
        title: 'HAVING clause',
        database: 'northwind',
        description: 'Filter aggregated results - categories with 2+ products',
        sqlFeatures: ['GROUP BY', 'HAVING', 'COUNT']
      },
      {
        id: 'agg-4',
        title: 'Multiple grouping columns',
        database: 'employees',
        description: 'Group by department and title with salary aggregates',
        sqlFeatures: ['GROUP BY', 'HAVING', 'AVG', 'COUNT']
      }
    ]
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
        description: 'Build complete org chart using recursive CTE',
        sqlFeatures: ['WITH RECURSIVE', 'UNION ALL', 'INNER JOIN']
      },
      {
        id: 'rec-2',
        title: 'Count hierarchy levels',
        database: 'employees',
        description: 'Count employees at each level of the org chart',
        sqlFeatures: ['WITH RECURSIVE', 'UNION ALL', 'GROUP BY']
      }
    ]
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
        description: 'Find products above average price',
        sqlFeatures: ['Subquery', 'AVG', 'WHERE', 'SELECT']
      },
      {
        id: 'sub-2',
        title: 'IN subquery',
        database: 'northwind',
        description: 'Find products in specific categories using IN',
        sqlFeatures: ['IN', 'Subquery', 'WHERE']
      },
      {
        id: 'sub-3',
        title: 'Subquery in FROM clause',
        database: 'employees',
        description: 'Use subquery as derived table for further processing',
        sqlFeatures: ['Subquery in FROM', 'CASE', 'AVG', 'GROUP BY']
      }
    ]
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
        description: 'Categorize products by price range',
        sqlFeatures: ['CASE', 'WHEN', 'ELSE', 'END']
      },
      {
        id: 'case-2',
        title: 'CASE in aggregation',
        database: 'employees',
        description: 'Conditional counting using CASE expressions',
        sqlFeatures: ['CASE', 'COUNT', 'GROUP BY']
      },
      {
        id: 'case-3',
        title: 'Multiple CASE expressions',
        database: 'employees',
        description: 'Multiple CASE statements for complex categorization',
        sqlFeatures: ['CASE', 'String concatenation', 'LIMIT']
      }
    ]
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
        description: 'Combine unique departments from two criteria',
        sqlFeatures: ['UNION', 'WHERE', 'ORDER BY']
      },
      {
        id: 'set-2',
        title: 'UNION ALL',
        database: 'northwind',
        description: 'Combine expensive and cheap products (with duplicates)',
        sqlFeatures: ['UNION ALL', 'WHERE', 'String literals']
      },
      {
        id: 'set-3',
        title: 'Column count in UNION',
        database: 'northwind',
        description: 'Union different tables with matching column structures',
        sqlFeatures: ['UNION', 'AS', 'LIMIT']
      }
    ]
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
        description: 'Show employee count and salary statistics by department',
        sqlFeatures: ['LEFT JOIN', 'GROUP BY', 'COUNT', 'AVG', 'MIN', 'MAX']
      },
      {
        id: 'company-2',
        title: 'Total project budget by department',
        database: 'company',
        description: 'Calculate total project budgets and counts per department',
        sqlFeatures: ['LEFT JOIN', 'GROUP BY', 'COUNT', 'SUM', 'HAVING']
      },
      {
        id: 'company-3',
        title: 'Employee project assignments',
        database: 'company',
        description: 'Show employees with their departments and assigned projects',
        sqlFeatures: ['LEFT JOIN', 'Multi-table JOIN', 'WHERE', 'LIMIT']
      },
      {
        id: 'company-4',
        title: 'Budget efficiency analysis',
        database: 'company',
        description: 'Calculate budget efficiency (budget per employee) by department',
        sqlFeatures: ['LEFT JOIN', 'Multi-table JOIN', 'GROUP BY', 'CASE', 'Calculated fields']
      },
      {
        id: 'company-5',
        title: 'High-value projects report',
        database: 'company',
        description: 'List projects with budget categorization and department info',
        sqlFeatures: ['LEFT JOIN', 'WHERE', 'CASE', 'ORDER BY']
      }
    ]
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
        description: 'Calculate GPA from enrollments using correlated subquery',
        sqlFeatures: ['Correlated subquery', 'CASE', 'AVG', 'NULL handling']
      },
      {
        id: 'uni-2',
        title: 'Course Enrollment Statistics',
        database: 'university',
        description: 'Count enrollments by course with LEFT JOIN',
        sqlFeatures: ['LEFT JOIN', 'COUNT', 'GROUP BY', 'ORDER BY']
      },
      {
        id: 'uni-3',
        title: 'Department Analysis',
        database: 'university',
        description: 'Multi-table JOIN to analyze departments',
        sqlFeatures: ['Multi-table JOIN', 'COUNT DISTINCT', 'AVG', 'GROUP BY']
      },
      {
        id: 'uni-4',
        title: 'Grade Distribution',
        database: 'university',
        description: 'Count enrollments by grade (excluding NULLs)',
        sqlFeatures: ['COUNT', 'GROUP BY', 'WHERE', 'NULL filtering']
      },
      {
        id: 'uni-5',
        title: 'High-Performing Students',
        database: 'university',
        description: 'Students with GPA above their major average',
        sqlFeatures: ['Correlated subquery', 'AVG', 'WHERE', 'NULL handling']
      },
      {
        id: 'uni-6',
        title: 'Courses with No Enrollments',
        database: 'university',
        description: 'Find courses with zero enrollment using LEFT JOIN',
        sqlFeatures: ['LEFT JOIN', 'WHERE', 'NULL filtering']
      }
    ]
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
        description: 'Create a simple table with basic column definitions',
        sqlFeatures: ['CREATE TABLE', 'INTEGER', 'VARCHAR', 'FLOAT']
      },
      {
        id: 'ddl-2',
        title: 'PRIMARY KEY Constraint',
        database: 'empty',
        description: 'Create table with PRIMARY KEY to ensure unique identification',
        sqlFeatures: ['CREATE TABLE', 'PRIMARY KEY', 'INSERT']
      },
      {
        id: 'ddl-3',
        title: 'PRIMARY KEY Violation',
        database: 'empty',
        description: 'Attempt to insert duplicate PRIMARY KEY value (will fail)',
        sqlFeatures: ['PRIMARY KEY', 'Constraint Enforcement']
      },
      {
        id: 'ddl-4',
        title: 'UNIQUE Constraint',
        database: 'empty',
        description: 'Create table with UNIQUE constraint on email column',
        sqlFeatures: ['UNIQUE', 'PRIMARY KEY', 'Constraint Enforcement']
      },
      {
        id: 'ddl-5',
        title: 'UNIQUE Constraint Violation',
        database: 'empty',
        description: 'Attempt to insert duplicate UNIQUE value (will fail)',
        sqlFeatures: ['UNIQUE', 'Constraint Enforcement']
      },
      {
        id: 'ddl-6',
        title: 'CHECK Constraint',
        database: 'empty',
        description: 'Create table with CHECK constraints to validate data ranges',
        sqlFeatures: ['CHECK', 'Constraint Enforcement', 'Expressions']
      },
      {
        id: 'ddl-7',
        title: 'CHECK Constraint Violation',
        database: 'empty',
        description: 'Attempt to insert value that violates CHECK constraint (will fail)',
        sqlFeatures: ['CHECK', 'Constraint Enforcement']
      },
      {
        id: 'ddl-8',
        title: 'Multiple Constraints',
        database: 'empty',
        description: 'Combine PRIMARY KEY, UNIQUE, and multiple CHECK constraints',
        sqlFeatures: ['PRIMARY KEY', 'UNIQUE', 'CHECK', 'Multiple Constraints']
      },
      {
        id: 'ddl-9',
        title: 'UPDATE with Constraint Checks',
        database: 'empty',
        description: 'Constraints are enforced during UPDATE operations',
        sqlFeatures: ['UPDATE', 'CHECK', 'Constraint Enforcement']
      },
      {
        id: 'ddl-10',
        title: 'UPDATE Constraint Violation',
        database: 'empty',
        description: 'UPDATE that violates CHECK constraint (will fail)',
        sqlFeatures: ['UPDATE', 'CHECK', 'Constraint Enforcement']
      },
      {
        id: 'ddl-11',
        title: 'Composite PRIMARY KEY',
        database: 'empty',
        description: 'PRIMARY KEY spanning multiple columns for unique combinations',
        sqlFeatures: ['PRIMARY KEY', 'Composite Key', 'Multi-column Constraints']
      },
      {
        id: 'ddl-12',
        title: 'Real-World: User Registration',
        database: 'empty',
        description: 'Real-world user registration table with multiple constraint types',
        sqlFeatures: ['PRIMARY KEY', 'UNIQUE', 'CHECK', 'Business Logic']
      }
    ]
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
        description: 'Create table and insert individual rows',
        sqlFeatures: ['CREATE TABLE', 'INSERT', 'VALUES', 'SELECT']
      },
      {
        id: 'dml-2',
        title: 'INSERT with Column List',
        database: 'empty',
        description: 'Insert with explicit column specification for partial data',
        sqlFeatures: ['INSERT', 'Column list', 'Partial columns', 'NULL handling']
      },
      {
        id: 'dml-3',
        title: 'Multiple Row INSERT',
        database: 'empty',
        description: 'Batch insert multiple rows in a single statement',
        sqlFeatures: ['INSERT', 'Multi-row VALUES', 'Batch insertion']
      },
      {
        id: 'dml-4',
        title: 'Basic UPDATE',
        database: 'empty',
        description: 'Update table data with calculated values',
        sqlFeatures: ['UPDATE', 'SET', 'WHERE', 'Calculated updates']
      },
      {
        id: 'dml-5',
        title: 'UPDATE Multiple Columns',
        database: 'empty',
        description: 'Update multiple columns simultaneously with calculations',
        sqlFeatures: ['UPDATE', 'Multiple SET clauses', 'Calculations']
      },
      {
        id: 'dml-6',
        title: 'UPDATE with Conditional Logic',
        database: 'empty',
        description: 'Apply tiered discounts based on price using CASE',
        sqlFeatures: ['UPDATE', 'CASE', 'Business logic', 'ROUND']
      },
      {
        id: 'dml-7',
        title: 'Simple DELETE',
        database: 'empty',
        description: 'Remove rows matching a simple condition',
        sqlFeatures: ['DELETE', 'WHERE', 'Conditional deletion']
      },
      {
        id: 'dml-8',
        title: 'DELETE with Complex Condition',
        database: 'empty',
        description: 'Remove rows using compound conditions with OR',
        sqlFeatures: ['DELETE', 'Complex WHERE', 'OR', 'Multiple conditions']
      },
      {
        id: 'dml-9',
        title: 'Complete Workflow (CRUD)',
        database: 'empty',
        description: 'Demonstrate full CRUD lifecycle in a single workflow',
        sqlFeatures: ['CRUD', 'INSERT', 'SELECT', 'UPDATE', 'DELETE', 'Business workflow']
      },
      {
        id: 'dml-10',
        title: 'Transaction-style Operations',
        database: 'empty',
        description: 'Simulate money transfer with multi-step updates and tiering',
        sqlFeatures: ['UPDATE', 'CHECK constraint', 'Multi-step operations', 'CASE', 'Business logic']
      }
    ]
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
        description: 'Get current date, time, and timestamp values with aliases',
        sqlFeatures: ['CURRENT_DATE', 'CURDATE', 'CURRENT_TIME', 'CURTIME', 'CURRENT_TIMESTAMP', 'NOW']
      },
      {
        id: 'datetime-2',
        title: 'Date Part Extraction',
        database: 'employees',
        description: 'Extract year, month, and day components from dates',
        sqlFeatures: ['YEAR', 'MONTH', 'DAY', 'CURRENT_DATE']
      },
      {
        id: 'datetime-3',
        title: 'Time Part Extraction',
        database: 'employees',
        description: 'Extract hour, minute, and second components from timestamps',
        sqlFeatures: ['HOUR', 'MINUTE', 'SECOND', 'CURRENT_TIMESTAMP']
      },
      {
        id: 'datetime-4',
        title: 'Employee Hire Date Analysis',
        database: 'employees',
        description: 'Analyze employee hire dates with year and month extraction',
        sqlFeatures: ['YEAR', 'MONTH', 'String concatenation', 'ORDER BY']
      },
      {
        id: 'datetime-5',
        title: 'Grouping by Date Parts',
        database: 'employees',
        description: 'Group and count employees by hire year and month',
        sqlFeatures: ['YEAR', 'MONTH', 'GROUP BY', 'COUNT', 'Date functions in GROUP BY']
      },
      {
        id: 'datetime-6',
        title: 'Date Filtering',
        database: 'employees',
        description: 'Filter employees hired in recent years using date functions',
        sqlFeatures: ['YEAR', 'WHERE', 'Date functions in WHERE']
      },
      {
        id: 'datetime-7',
        title: 'Age/Tenure Calculation',
        database: 'employees',
        description: 'Calculate employee tenure and categorize by experience level',
        sqlFeatures: ['YEAR', 'CURRENT_DATE', 'CASE', 'Date arithmetic']
      },
      {
        id: 'datetime-8',
        title: 'Date-based Aggregations',
        database: 'employees',
        description: 'Aggregate employee statistics by hire year and department',
        sqlFeatures: ['YEAR', 'GROUP BY', 'HAVING', 'COUNT', 'AVG', 'Date-based reporting']
      },
      {
        id: 'datetime-9',
        title: 'Quarter Analysis',
        database: 'employees',
        description: 'Analyze hiring patterns by fiscal quarter using date functions',
        sqlFeatures: ['MONTH', 'YEAR', 'CASE', 'BETWEEN', 'GROUP BY', 'Business intelligence']
      }
    ]
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
        description: 'Match products starting with "Ch" using % wildcard',
        sqlFeatures: ['LIKE', '% wildcard', 'Pattern matching']
      },
      {
        id: 'pattern-2',
        title: 'LIKE with Multiple Patterns',
        database: 'northwind',
        description: 'Find products containing "sauce" or "cream" (case-insensitive)',
        sqlFeatures: ['LIKE', 'OR', 'Case-insensitive matching']
      },
      {
        id: 'pattern-3',
        title: 'LIKE with Underscore Wildcard',
        database: 'employees',
        description: 'Match names like "John" using _ for single character',
        sqlFeatures: ['LIKE', '_ wildcard', 'Single character matching']
      },
      {
        id: 'pattern-4',
        title: 'BETWEEN for Ranges',
        database: 'northwind',
        description: 'Find products with prices in the 10-50 range (inclusive)',
        sqlFeatures: ['BETWEEN', 'Numeric ranges', 'Inclusive bounds']
      },
      {
        id: 'pattern-5',
        title: 'BETWEEN with Dates',
        database: 'employees',
        description: 'Filter employees hired between 2019-2021 using date functions',
        sqlFeatures: ['BETWEEN', 'YEAR', 'Date ranges']
      },
      {
        id: 'pattern-6',
        title: 'IN with List',
        database: 'northwind',
        description: 'Find products in specific categories using IN operator',
        sqlFeatures: ['IN', 'List of values', 'Multi-column ORDER BY']
      },
      {
        id: 'pattern-7',
        title: 'IN with Subquery',
        database: 'northwind',
        description: 'Use IN with subquery to find products in Beverage categories',
        sqlFeatures: ['IN', 'Subquery', 'LIKE', 'Dynamic filtering']
      },
      {
        id: 'pattern-8',
        title: 'EXISTS with Correlated Subquery',
        database: 'northwind',
        description: 'Find categories that have expensive products (>$50)',
        sqlFeatures: ['EXISTS', 'Correlated subquery', 'Existential check']
      },
      {
        id: 'pattern-9',
        title: 'NOT IN and NOT BETWEEN',
        database: 'northwind',
        description: 'Find products outside price range and excluded categories',
        sqlFeatures: ['NOT BETWEEN', 'NOT IN', 'Negative predicates', 'LIMIT']
      },
      {
        id: 'pattern-10',
        title: 'Combining Predicates',
        database: 'northwind',
        description: 'Complex filtering using multiple predicates together',
        sqlFeatures: ['LIKE', 'BETWEEN', 'IN', 'AND', 'Multiple predicates']
      },
      {
        id: 'pattern-11',
        title: 'ALL Quantified Comparison',
        database: 'northwind',
        description: 'Find products more expensive than ALL category 1 products',
        sqlFeatures: ['ALL', 'Quantified comparison', 'Subquery', 'Universal quantifier']
      },
      {
        id: 'pattern-12',
        title: 'ANY/SOME Quantified Comparison',
        database: 'northwind',
        description: 'Find products more expensive than ANY category average',
        sqlFeatures: ['ANY', 'SOME', 'Quantified comparison', 'AVG', 'GROUP BY', 'Existential quantifier']
      }
    ]
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
        description: 'Calculate absolute values and sign indicators for price differences',
        sqlFeatures: ['ABS', 'SIGN', 'Expressions']
      },
      {
        id: 'math-2',
        title: 'Rounding Functions',
        database: 'northwind',
        description: 'Demonstrate various rounding methods for numeric values',
        sqlFeatures: ['ROUND', 'FLOOR', 'CEIL', 'WHERE']
      },
      {
        id: 'math-3',
        title: 'Power and Square Root',
        database: 'northwind',
        description: 'Calculate squares, square roots, and combinations',
        sqlFeatures: ['POWER', 'SQRT', 'ROUND', 'WHERE']
      },
      {
        id: 'math-4',
        title: 'Modulo (Remainder)',
        database: 'northwind',
        description: 'Use MOD to divide products into rotating groups',
        sqlFeatures: ['MOD', 'CASE', 'ORDER BY']
      },
      {
        id: 'math-5',
        title: 'Logarithmic Functions',
        database: 'northwind',
        description: 'Calculate natural logarithms, base-10 logarithms, and exponentials',
        sqlFeatures: ['LN', 'LOG10', 'EXP', 'ROUND']
      },
      {
        id: 'math-6',
        title: 'Trigonometric Functions',
        database: 'northwind',
        description: 'Demonstrate trigonometric functions with common angles',
        sqlFeatures: ['PI', 'SIN', 'COS', 'TAN', 'ROUND']
      },
      {
        id: 'math-7',
        title: 'Inverse Trigonometric Functions',
        database: 'northwind',
        description: 'Calculate inverse trigonometric functions (arcsin, arccos, arctan)',
        sqlFeatures: ['ASIN', 'ACOS', 'ATAN', 'ATAN2', 'ROUND']
      },
      {
        id: 'math-8',
        title: 'Angle Conversions',
        database: 'northwind',
        description: 'Convert between degrees and radians',
        sqlFeatures: ['RADIANS', 'DEGREES', 'PI', 'ROUND']
      },
      {
        id: 'math-9',
        title: 'GREATEST and LEAST',
        database: 'northwind',
        description: 'Find maximum and minimum values across columns',
        sqlFeatures: ['GREATEST', 'LEAST', 'WHERE']
      },
      {
        id: 'math-10',
        title: 'Price Calculations',
        database: 'northwind',
        description: 'Practical business calculations: taxes, discounts, and pricing strategies',
        sqlFeatures: ['ROUND', 'FLOOR', 'String concatenation', 'Business calculations']
      }
    ]
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
        description: 'Replace NULL values with defaults using COALESCE',
        sqlFeatures: ['COALESCE', 'NULL defaults']
      },
      {
        id: 'null-2',
        title: 'COALESCE with Multiple Fallbacks',
        database: 'employees',
        description: 'Use COALESCE with multiple fallback values',
        sqlFeatures: ['COALESCE', 'Multiple fallback values']
      },
      {
        id: 'null-3',
        title: 'NULLIF to Convert Values to NULL',
        database: 'northwind',
        description: 'Convert specific values to NULL using NULLIF',
        sqlFeatures: ['NULLIF', 'CASE', 'IS NULL']
      },
      {
        id: 'null-4',
        title: 'COALESCE in Calculations',
        database: 'northwind',
        description: 'Use COALESCE for NULL-safe arithmetic operations',
        sqlFeatures: ['COALESCE', 'Calculated fields', 'NULL-safe arithmetic']
      },
      {
        id: 'null-5',
        title: 'COALESCE with Aggregates',
        database: 'northwind',
        description: 'Handle NULL results from LEFT JOIN aggregates with COALESCE',
        sqlFeatures: ['COALESCE', 'LEFT JOIN', 'AVG', 'SUM', 'GROUP BY', 'NULL handling in aggregates']
      },
      {
        id: 'null-6',
        title: 'Combining COALESCE and NULLIF',
        database: 'employees',
        description: 'Combine COALESCE and NULLIF for complex NULL logic',
        sqlFeatures: ['COALESCE', 'NULLIF', 'Complex NULL logic']
      },
      {
        id: 'null-7',
        title: 'NULL-Safe Comparisons',
        database: 'northwind',
        description: 'Use COALESCE for NULL-safe comparisons in business logic',
        sqlFeatures: ['COALESCE', 'CASE', 'NULL-safe comparisons', 'Business logic']
      },
      {
        id: 'null-8',
        title: 'COALESCE for Report Formatting',
        database: 'northwind',
        description: 'Format report output with COALESCE for NULL-safe string operations',
        sqlFeatures: ['COALESCE', 'CAST', 'String concatenation', 'Report formatting']
      }
    ]
  },
  {
    id: 'window',
    title: 'Window Functions',
    description: 'Aggregate window functions with OVER clause - running totals, moving averages, partitioned aggregates',
    queries: [
      {
        id: 'window-1',
        title: 'COUNT(*) OVER - Total Row Count',
        database: 'employees',
        description: 'Add total count to each row without GROUP BY collapse',
        sqlFeatures: ['COUNT', 'OVER', 'Window functions']
      },
      {
        id: 'window-2',
        title: 'Running Total with ORDER BY',
        database: 'employees',
        description: 'Calculate cumulative salary sum ordered by employee ID',
        sqlFeatures: ['SUM', 'OVER', 'ORDER BY', 'Running totals']
      },
      {
        id: 'window-3',
        title: 'Partitioned Averages',
        database: 'employees',
        description: 'Calculate average salary per department for each employee',
        sqlFeatures: ['AVG', 'OVER', 'PARTITION BY', 'Partitioned aggregates']
      },
      {
        id: 'window-4',
        title: 'MIN and MAX in Windows',
        database: 'northwind',
        description: 'Find min and max prices within each product category',
        sqlFeatures: ['MIN', 'MAX', 'OVER', 'PARTITION BY']
      },
      {
        id: 'window-5',
        title: 'Moving Average with Frame',
        database: 'employees',
        description: 'Calculate 3-row moving average of salaries',
        sqlFeatures: ['AVG', 'OVER', 'ROWS BETWEEN', 'Moving frame', 'Window frames']
      },
      {
        id: 'window-6',
        title: 'Window Function in Expression',
        database: 'employees',
        description: 'Calculate salary deviation from department average',
        sqlFeatures: ['AVG', 'OVER', 'PARTITION BY', 'Window functions in expressions']
      },
      {
        id: 'window-7',
        title: 'Multiple Window Functions',
        database: 'employees',
        description: 'Multiple aggregate window functions in one query',
        sqlFeatures: ['COUNT', 'AVG', 'MIN', 'MAX', 'OVER', 'PARTITION BY', 'Multiple window functions']
      },
      {
        id: 'window-8',
        title: 'Percentage of Total',
        database: 'northwind',
        description: 'Calculate each product price as percentage of total',
        sqlFeatures: ['SUM', 'OVER', 'ROUND', 'Percentage calculations', 'Business analytics']
      }
    ]
  },
  {
    id: 'business-intelligence',
    title: 'Business Intelligence Queries',
    description: 'Real-world analytics scenarios: YoY growth, RFM analysis, sales funnels',
    queries: [
      {
        id: 'bi-1',
        title: 'Year-over-Year Growth Analysis',
        database: 'northwind',
        description: 'Calculate year-over-year growth in order volume',
        sqlFeatures: ['EXTRACT', 'LAG', 'OVER', 'CASE', 'Window functions', 'Date functions'],
        difficulty: 'intermediate',
        useCase: 'analytics',
        relatedExamples: ['aggregate-1', 'window-1'],
        tags: ['yoy', 'growth', 'trends', 'business-metrics']
      },
      {
        id: 'bi-2',
        title: 'Customer Segmentation (RFM Analysis)',
        database: 'northwind',
        description: 'Segment customers by purchase behavior (RFM model)',
        sqlFeatures: ['CTE', 'Aggregates', 'CASE', 'Segmentation'],
        difficulty: 'advanced',
        useCase: 'analytics',
        relatedExamples: ['cte-1', 'aggregate-1'],
        tags: ['rfm', 'segmentation', 'customer-value', 'crm']
      },
      {
        id: 'bi-3',
        title: 'Sales Funnel Analysis',
        database: 'northwind',
        description: 'Analyze sales funnel metrics by product category',
        sqlFeatures: ['CTE', 'JOINs', 'Aggregates', 'NULLIF', 'Business metrics'],
        difficulty: 'intermediate',
        useCase: 'analytics',
        relatedExamples: ['join-1', 'aggregate-1'],
        tags: ['funnel', 'conversion', 'sales-metrics', 'category-analysis']
      },
      {
        id: 'bi-4',
        title: 'Cohort Analysis',
        database: 'northwind',
        description: 'Track customer retention over time using cohort analysis',
        sqlFeatures: ['CTE', 'DATE_TRUNC', 'FIRST_VALUE', 'Window functions', 'Cohort analysis'],
        difficulty: 'advanced',
        useCase: 'analytics',
        relatedExamples: ['cte-1', 'window-1'],
        tags: ['cohort', 'retention', 'customer-lifetime-value', 'churn']
      },
      {
        id: 'bi-5',
        title: 'Product Affinity Analysis',
        database: 'northwind',
        description: 'Identify products frequently purchased together (market basket analysis)',
        sqlFeatures: ['Self-join', 'CTE', 'Subquery', 'Market basket analysis'],
        difficulty: 'advanced',
        useCase: 'analytics',
        relatedExamples: ['join-5', 'subquery-1'],
        tags: ['affinity', 'cross-sell', 'market-basket', 'recommendations']
      },
      {
        id: 'bi-6',
        title: 'ABC Analysis (Inventory Classification)',
        database: 'northwind',
        description: 'Classify products by revenue contribution using ABC analysis',
        sqlFeatures: ['CTE', 'Window functions', 'SUM OVER', 'Classification'],
        difficulty: 'advanced',
        useCase: 'analytics',
        relatedExamples: ['window-1', 'cte-1'],
        tags: ['abc-analysis', 'pareto', 'inventory', 'revenue-analysis']
      },
      {
        id: 'bi-7',
        title: 'Pareto Analysis (80/20 Rule)',
        database: 'northwind',
        description: 'Apply Pareto principle to identify top revenue contributors',
        sqlFeatures: ['CTE', 'Window functions', 'Cumulative sums', 'Pareto analysis'],
        difficulty: 'intermediate',
        useCase: 'analytics',
        relatedExamples: ['bi-6', 'window-1'],
        tags: ['pareto', '80-20', 'revenue-contribution', 'vital-few']
      }
    ]
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
        description: 'Detect duplicate product names in the database',
        sqlFeatures: ['GROUP BY', 'HAVING', 'COUNT', 'STRING_AGG', 'Duplicate detection'],
        difficulty: 'beginner',
        useCase: 'data-quality',
        relatedExamples: ['aggregate-1'],
        tags: ['duplicates', 'validation', 'data-cleansing', 'integrity']
      },
      {
        id: 'dq-2',
        title: 'Identifying Orphaned Records',
        database: 'northwind',
        description: 'Detect order details without parent orders (referential integrity check)',
        sqlFeatures: ['LEFT JOIN', 'IS NULL', 'Referential integrity', 'Orphan detection'],
        difficulty: 'beginner',
        useCase: 'data-quality',
        relatedExamples: ['join-2'],
        tags: ['orphans', 'referential-integrity', 'foreign-keys', 'data-quality']
      },
      {
        id: 'dq-3',
        title: 'NULL Pattern Analysis',
        database: 'northwind',
        description: 'Analyze NULL value patterns across multiple columns',
        sqlFeatures: ['UNION ALL', 'COUNT', 'NULL analysis', 'Data profiling'],
        difficulty: 'intermediate',
        useCase: 'data-quality',
        relatedExamples: ['aggregate-1', 'union-1'],
        tags: ['null-analysis', 'data-profiling', 'completeness', 'quality-metrics']
      },
      {
        id: 'dq-4',
        title: 'Data Consistency Checks',
        database: 'northwind',
        description: 'Validate business rules for product data consistency',
        sqlFeatures: ['CASE', 'NULL checks', 'Data validation', 'Business rules'],
        difficulty: 'intermediate',
        useCase: 'data-quality',
        relatedExamples: ['case-1'],
        tags: ['consistency', 'validation', 'business-rules', 'constraints']
      },
      {
        id: 'dq-5',
        title: 'Referential Integrity Validation',
        database: 'northwind',
        description: 'Comprehensive validation of foreign key relationships',
        sqlFeatures: ['CTE', 'UNION ALL', 'LEFT JOIN', 'Referential integrity'],
        difficulty: 'intermediate',
        useCase: 'data-quality',
        relatedExamples: ['dq-2', 'cte-1'],
        tags: ['referential-integrity', 'foreign-keys', 'validation', 'constraints']
      },
      {
        id: 'dq-6',
        title: 'Outlier Detection',
        database: 'northwind',
        description: 'Identify price outliers using Z-score statistical method',
        sqlFeatures: ['CTE', 'STDDEV', 'AVG', 'CROSS JOIN', 'Statistical analysis'],
        difficulty: 'advanced',
        useCase: 'data-quality',
        relatedExamples: ['aggregate-1', 'cte-1'],
        tags: ['outliers', 'statistics', 'z-score', 'anomaly-detection']
      }
    ]
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
        description: 'Comprehensive category analysis combining CTEs, window functions, and JOINs',
        sqlFeatures: ['CTE', 'Window functions', 'RANK', 'Multiple JOINs', 'Complex aggregation'],
        difficulty: 'advanced',
        useCase: 'reports',
        relatedExamples: ['cte-1', 'window-1', 'join-1'],
        tags: ['complex-query', 'multi-feature', 'reporting', 'advanced-analytics']
      },
      {
        id: 'adv-2',
        title: 'Recursive CTEs + Aggregates',
        database: 'empty',
        description: 'Hierarchical data rollup using recursive CTEs and aggregation',
        sqlFeatures: ['Recursive CTE', 'Aggregates', 'STRING_AGG', 'Hierarchical queries'],
        difficulty: 'advanced',
        useCase: 'development',
        relatedExamples: ['recursive-1', 'cte-1'],
        tags: ['recursive', 'hierarchy', 'organizational-structure', 'tree-queries']
      },
      {
        id: 'adv-3',
        title: 'Subqueries in Multiple Contexts',
        database: 'northwind',
        description: 'Use subqueries in SELECT, FROM, WHERE, and EXISTS clauses',
        sqlFeatures: ['Scalar subquery', 'Correlated subquery', 'EXISTS', 'IN subquery'],
        difficulty: 'advanced',
        useCase: 'development',
        relatedExamples: ['subquery-1', 'subquery-2', 'subquery-3'],
        tags: ['subqueries', 'correlated', 'exists', 'multi-context']
      },
      {
        id: 'adv-4',
        title: 'Window Functions + CASE Expressions',
        database: 'northwind',
        description: 'Combine window functions with CASE expressions for conditional analytics',
        sqlFeatures: ['Window functions', 'CASE', 'Conditional aggregation', 'Multiple OVER clauses'],
        difficulty: 'advanced',
        useCase: 'analytics',
        relatedExamples: ['window-1', 'case-1'],
        tags: ['window-functions', 'case-expressions', 'conditional-logic', 'tiering']
      },
      {
        id: 'adv-5',
        title: 'Multiple CTEs with Cross-References',
        database: 'northwind',
        description: 'Multiple CTEs that reference each other for layered analysis',
        sqlFeatures: ['Multiple CTEs', 'CTE chaining', 'Window functions', 'Complex JOINs'],
        difficulty: 'advanced',
        useCase: 'analytics',
        relatedExamples: ['cte-1', 'cte-2', 'window-1'],
        tags: ['cte-chaining', 'layered-analysis', 'complex-queries', 'data-pipeline']
      }
    ]
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
        description: 'Demonstrate SQL:1999 CASE expressions (simple and searched forms)',
        sqlFeatures: ['CASE', 'Simple CASE', 'Searched CASE', 'SQL:1999 standard'],
        difficulty: 'beginner',
        useCase: 'development',
        relatedExamples: ['case-1'],
        tags: ['sql1999', 'case-expression', 'conditional-logic', 'standards']
      },
      {
        id: 'std-2',
        title: 'Common Table Expressions (SQL:1999)',
        database: 'northwind',
        description: 'Use SQL:1999 Common Table Expressions (WITH clause)',
        sqlFeatures: ['CTE', 'WITH', 'SQL:1999 standard', 'Query modularity'],
        difficulty: 'intermediate',
        useCase: 'development',
        relatedExamples: ['cte-1'],
        tags: ['sql1999', 'cte', 'with-clause', 'standards']
      },
      {
        id: 'std-3',
        title: 'Window Functions (SQL:1999)',
        database: 'northwind',
        description: 'Demonstrate SQL:1999 window functions (OVER clause)',
        sqlFeatures: ['Window functions', 'ROW_NUMBER', 'RANK', 'DENSE_RANK', 'OVER', 'SQL:1999'],
        difficulty: 'intermediate',
        useCase: 'analytics',
        relatedExamples: ['window-1', 'window-2'],
        tags: ['sql1999', 'window-functions', 'over-clause', 'ranking']
      },
      {
        id: 'std-4',
        title: 'BOOLEAN Data Type (SQL:1999)',
        database: 'empty',
        description: 'Use SQL:1999 BOOLEAN data type and boolean operations',
        sqlFeatures: ['BOOLEAN', 'Boolean algebra', 'AND', 'OR', 'NOT', 'SQL:1999'],
        difficulty: 'beginner',
        useCase: 'development',
        relatedExamples: ['datatype-1'],
        tags: ['sql1999', 'boolean', 'data-types', 'logic']
      },
      {
        id: 'std-5',
        title: 'Standards-Compliant Date Functions',
        database: 'northwind',
        description: 'Use SQL:1999 standards-compliant date/time functions',
        sqlFeatures: ['EXTRACT', 'INTERVAL', 'DATE_TRUNC', 'Date functions', 'SQL:1999'],
        difficulty: 'intermediate',
        useCase: 'development',
        relatedExamples: ['datetime-1'],
        tags: ['sql1999', 'date-functions', 'temporal', 'standards']
      },
      {
        id: 'std-6',
        title: 'NULLIF and COALESCE (SQL:1999)',
        database: 'northwind',
        description: 'Demonstrate SQL:1999 NULLIF and COALESCE functions',
        sqlFeatures: ['NULLIF', 'COALESCE', 'NULL handling', 'SQL:1999'],
        difficulty: 'intermediate',
        useCase: 'development',
        relatedExamples: ['null-1'],
        tags: ['sql1999', 'null-handling', 'coalesce', 'nullif']
      }
    ]
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
        description: 'Demonstrate efficient set-based operations (vs row-by-row processing)',
        sqlFeatures: ['Aggregates', 'GROUP BY', 'Set-based operations', 'Performance'],
        difficulty: 'beginner',
        useCase: 'development',
        performanceNotes: 'Set-based operations are 10-100x faster than iterative approaches',
        relatedExamples: ['aggregate-1'],
        tags: ['performance', 'set-based', 'optimization', 'best-practices']
      },
      {
        id: 'perf-2',
        title: 'JOIN vs Subquery Performance',
        database: 'northwind',
        description: 'Compare JOIN vs correlated subquery performance',
        sqlFeatures: ['JOIN', 'Subquery comparison', 'Query optimization'],
        difficulty: 'intermediate',
        useCase: 'development',
        performanceNotes: 'JOINs allow optimizer flexibility; correlated subqueries may execute once per row',
        relatedExamples: ['join-1', 'subquery-1'],
        tags: ['performance', 'join-vs-subquery', 'optimization', 'query-patterns']
      },
      {
        id: 'perf-3',
        title: 'Index-Friendly Query Patterns',
        database: 'northwind',
        description: 'Demonstrate index-friendly (SARGable) vs index-unfriendly queries',
        sqlFeatures: ['WHERE', 'Index optimization', 'SARGable predicates'],
        difficulty: 'intermediate',
        useCase: 'development',
        performanceNotes: 'SARGable predicates enable index usage; functions on columns prevent it',
        relatedExamples: ['basic-2'],
        tags: ['performance', 'indexes', 'sargable', 'optimization']
      },
      {
        id: 'perf-4',
        title: 'Efficient Aggregation with CTEs',
        database: 'northwind',
        description: 'Use CTEs for efficient pre-aggregation',
        sqlFeatures: ['CTE', 'Pre-aggregation', 'JOIN', 'Performance optimization'],
        difficulty: 'intermediate',
        useCase: 'development',
        performanceNotes: 'CTE pre-aggregates once; correlated subqueries repeat for each row',
        relatedExamples: ['cte-1', 'aggregate-1'],
        tags: ['performance', 'cte', 'aggregation', 'optimization']
      },
      {
        id: 'perf-5',
        title: 'Avoiding SELECT *',
        database: 'northwind',
        description: 'Select specific columns instead of SELECT *',
        sqlFeatures: ['SELECT', 'Column selection', 'Performance best practices'],
        difficulty: 'beginner',
        useCase: 'development',
        performanceNotes: 'Explicit column lists reduce I/O and enable covering indexes',
        relatedExamples: ['basic-1'],
        tags: ['performance', 'select-star', 'best-practices', 'optimization']
      },
      {
        id: 'perf-6',
        title: 'Efficient LIMIT with ORDER BY',
        database: 'northwind',
        description: 'Efficient use of LIMIT with ORDER BY for top-N queries',
        sqlFeatures: ['LIMIT', 'ORDER BY', 'Top-N optimization'],
        difficulty: 'beginner',
        useCase: 'development',
        performanceNotes: 'LIMIT allows early termination; large OFFSETs still process skipped rows',
        relatedExamples: ['basic-1', 'basic-2'],
        tags: ['performance', 'limit', 'top-n', 'pagination']
      }
    ]
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
        description: 'Complete monthly sales summary with KPIs and growth metrics',
        sqlFeatures: ['CTE', 'DATE_TRUNC', 'Aggregates', 'LAG', 'Period-over-period'],
        difficulty: 'intermediate',
        useCase: 'reports',
        relatedExamples: ['bi-1', 'window-1'],
        tags: ['monthly-report', 'kpi', 'executive-summary', 'sales-metrics']
      },
      {
        id: 'rpt-2',
        title: 'Top N Per Group Report',
        database: 'northwind',
        description: 'Top performers by category (classic top-N-per-group pattern)',
        sqlFeatures: ['CTE', 'ROW_NUMBER', 'PARTITION BY', 'Top-N per group'],
        difficulty: 'intermediate',
        useCase: 'reports',
        relatedExamples: ['window-2', 'cte-1'],
        tags: ['top-n-per-group', 'ranking', 'category-analysis', 'product-performance']
      },
      {
        id: 'rpt-3',
        title: 'Running Totals and Cumulative Sums',
        database: 'northwind',
        description: 'Daily metrics with running totals and moving averages',
        sqlFeatures: ['CTE', 'Window functions', 'Running totals', 'Moving averages', 'ROWS BETWEEN'],
        difficulty: 'advanced',
        useCase: 'reports',
        relatedExamples: ['window-1', 'window-5'],
        tags: ['running-total', 'cumulative', 'moving-average', 'time-series']
      },
      {
        id: 'rpt-4',
        title: 'Period-over-Period Comparison',
        database: 'northwind',
        description: 'Quarterly sales with quarter-over-quarter and year-over-year comparisons',
        sqlFeatures: ['CTE', 'EXTRACT', 'LAG', 'Period comparisons', 'Window functions'],
        difficulty: 'advanced',
        useCase: 'reports',
        relatedExamples: ['bi-1', 'window-1'],
        tags: ['period-comparison', 'qoq', 'yoy', 'quarterly-report', 'trends']
      },
      {
        id: 'rpt-5',
        title: 'Pivot Table Simulation',
        database: 'northwind',
        description: 'Simulate pivot table using CASE and GROUP BY (categories by quarters)',
        sqlFeatures: ['CTE', 'CASE', 'Conditional aggregation', 'Pivot simulation'],
        difficulty: 'advanced',
        useCase: 'reports',
        relatedExamples: ['aggregate-1', 'case-1'],
        tags: ['pivot-table', 'crosstab', 'matrix-report', 'conditional-aggregation']
      },
      {
        id: 'rpt-6',
        title: 'Executive Dashboard Summary',
        database: 'northwind',
        description: 'Executive dashboard with key business metrics and KPIs',
        sqlFeatures: ['CTE', 'UNION ALL', 'Aggregates', 'KPI reporting'],
        difficulty: 'intermediate',
        useCase: 'reports',
        relatedExamples: ['aggregate-1', 'union-1'],
        tags: ['dashboard', 'kpi', 'executive-summary', 'metrics']
      }
    ]
  }

]

/**
 * Get all example metadata flattened
 */
export function getAllExampleMetadata(): ExampleMetadata[] {
  return exampleCategoriesMetadata.flatMap(cat => cat.queries)
}

/**
 * Find metadata by ID
 */
export function findExampleMetadata(id: string): ExampleMetadata | undefined {
  return getAllExampleMetadata().find(ex => ex.id === id)
}

/**
 * Get metadata for examples of a specific database
 */
export function getExampleMetadataForDatabase(
  database: 'northwind' | 'employees' | 'company' | 'university' | 'empty'
): ExampleMetadata[] {
  return getAllExampleMetadata().filter(ex => ex.database === database)
}

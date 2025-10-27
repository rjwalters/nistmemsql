export interface SubqueryType {
  name: string
  category: 'Scalar' | 'Table' | 'Predicate' | 'Correlated' | 'Planned'
  status: 'implemented' | 'planned'
  description: string
  syntax: string
  useCase: string
}

export const SUBQUERY_TYPES: SubqueryType[] = [
  // Scalar Subqueries
  {
    name: 'Scalar Subquery in SELECT',
    category: 'Scalar',
    status: 'implemented',
    description: 'Returns single value for use in SELECT list',
    syntax: 'SELECT col, (SELECT ...) AS subquery_col FROM table',
    useCase: 'Computing aggregate values alongside detail rows',
  },
  {
    name: 'Scalar Subquery in WHERE',
    category: 'Scalar',
    status: 'implemented',
    description: 'Returns single value for comparison in WHERE clause',
    syntax: 'SELECT ... WHERE col > (SELECT ...)',
    useCase: 'Filtering based on aggregated or computed values',
  },

  // Table Subqueries
  {
    name: 'Derived Table',
    category: 'Table',
    status: 'implemented',
    description: 'Subquery in FROM clause acts as virtual table',
    syntax: 'SELECT ... FROM (SELECT ...) AS alias',
    useCase: 'Breaking complex queries into logical steps',
  },
  {
    name: 'Derived Table with Aggregation',
    category: 'Table',
    status: 'implemented',
    description: 'Aggregate data before joining or filtering',
    syntax: 'SELECT ... FROM (SELECT COUNT(*) ...) AS stats',
    useCase: 'Pre-aggregating data for further analysis',
  },

  // Predicate Subqueries
  {
    name: 'IN Subquery',
    category: 'Predicate',
    status: 'implemented',
    description: 'Check if value exists in subquery results',
    syntax: 'SELECT ... WHERE col IN (SELECT ...)',
    useCase: 'Filtering based on membership in another table',
  },
  {
    name: 'NOT IN Subquery',
    category: 'Predicate',
    status: 'implemented',
    description: 'Check if value does not exist in subquery results',
    syntax: 'SELECT ... WHERE col NOT IN (SELECT ...)',
    useCase: 'Finding records that lack a relationship',
  },

  // Correlated Subqueries
  {
    name: 'Correlated Subquery',
    category: 'Correlated',
    status: 'implemented',
    description: 'Inner query references columns from outer query',
    syntax: 'SELECT ... WHERE col > (SELECT ... WHERE outer.id = inner.id)',
    useCase: 'Row-by-row comparisons with related data',
  },
  {
    name: 'Nested Subqueries',
    category: 'Correlated',
    status: 'implemented',
    description: 'Multiple levels of subquery nesting',
    syntax: 'SELECT ... WHERE col IN (SELECT ... WHERE ... IN (SELECT ...))',
    useCase: 'Complex multi-level filtering',
  },

  // Planned Features
  {
    name: 'EXISTS Predicate',
    category: 'Planned',
    status: 'planned',
    description: 'Check if subquery returns any rows',
    syntax: 'SELECT ... WHERE EXISTS (SELECT ...)',
    useCase: 'Testing for existence without caring about values',
  },
  {
    name: 'Row Subqueries',
    category: 'Planned',
    status: 'planned',
    description: 'Subquery returns multiple columns for row comparison',
    syntax: 'SELECT ... WHERE (col1, col2) = (SELECT col1, col2 ...)',
    useCase: 'Comparing multiple columns simultaneously',
  },
  {
    name: 'Quantified Comparisons (ALL/SOME/ANY)',
    category: 'Planned',
    status: 'planned',
    description: 'Compare value against all/some values in subquery',
    syntax: 'SELECT ... WHERE col > ALL (SELECT ...)',
    useCase: 'Finding maximum, minimum, or threshold matches',
  },
]

export const SUBQUERY_EXAMPLES = {
  scalarInSelect: `CREATE TABLE employees (
    id INTEGER,
    name VARCHAR(50),
    salary INTEGER
);

INSERT INTO employees VALUES (1, 'Alice', 95000);
INSERT INTO employees VALUES (2, 'Bob', 75000);
INSERT INTO employees VALUES (3, 'Charlie', 105000);

-- Scalar subquery in SELECT
SELECT name, salary,
       (SELECT AVG(salary) FROM employees) AS avg_salary
FROM employees;`,

  scalarInWhere: `-- Scalar subquery in WHERE
SELECT name FROM employees
WHERE salary > (SELECT AVG(salary) FROM employees);

-- Expected: Alice, Charlie (above average)`,

  derivedTable: `-- Subquery in FROM clause
SELECT high_earners.name, high_earners.salary
FROM (
    SELECT name, salary
    FROM employees
    WHERE salary > 80000
) AS high_earners;

-- Derived table with aggregation
SELECT dept_stats.avg_sal
FROM (
    SELECT AVG(salary) AS avg_sal
    FROM employees
) AS dept_stats;`,

  inPredicate: `CREATE TABLE managers (
    manager_id INTEGER
);

INSERT INTO managers VALUES (1);
INSERT INTO managers VALUES (3);

-- IN with subquery
SELECT name FROM employees
WHERE id IN (SELECT manager_id FROM managers);

-- NOT IN with subquery
SELECT name FROM employees
WHERE id NOT IN (SELECT manager_id FROM managers);`,

  correlated: `CREATE TABLE departments (
    dept_id INTEGER,
    dept_name VARCHAR(50)
);

CREATE TABLE dept_employees (
    emp_id INTEGER,
    name VARCHAR(50),
    dept_id INTEGER,
    salary INTEGER
);

INSERT INTO departments VALUES (1, 'Engineering');
INSERT INTO departments VALUES (2, 'Sales');

INSERT INTO dept_employees VALUES (1, 'Alice', 1, 95000);
INSERT INTO dept_employees VALUES (2, 'Bob', 1, 85000);
INSERT INTO dept_employees VALUES (3, 'Charlie', 2, 75000);

-- Find employees earning more than their department average
SELECT name, salary
FROM dept_employees e1
WHERE salary > (
    SELECT AVG(salary)
    FROM dept_employees e2
    WHERE e2.dept_id = e1.dept_id
);`,

  nested: `-- Multiple levels of nesting
SELECT name FROM employees
WHERE salary > (
    SELECT AVG(salary) FROM employees
    WHERE id IN (SELECT manager_id FROM managers)
);

-- Complex nested subqueries
SELECT name FROM employees
WHERE id IN (
    SELECT manager_id FROM managers
    WHERE manager_id IN (
        SELECT id FROM employees WHERE salary > 90000
    )
);`,
}

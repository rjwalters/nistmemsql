export interface JoinType {
  name: string
  category: 'Inner' | 'Outer' | 'Cross' | 'Multi'
  status: 'implemented' | 'planned'
  description: string
  syntax: string
  useCase: string
}

export const JOIN_TYPES: JoinType[] = [
  // Inner Joins
  {
    name: 'INNER JOIN',
    category: 'Inner',
    status: 'implemented',
    description: 'Returns only matching rows from both tables',
    syntax: 'SELECT ... FROM table1 INNER JOIN table2 ON condition',
    useCase: 'When you only want records that exist in both tables',
  },

  // Outer Joins
  {
    name: 'LEFT OUTER JOIN',
    category: 'Outer',
    status: 'implemented',
    description:
      'Returns all rows from left table, with matches from right (NULLs for non-matches)',
    syntax: 'SELECT ... FROM table1 LEFT JOIN table2 ON condition',
    useCase: 'When you want all records from the first table, even without matches',
  },
  {
    name: 'RIGHT OUTER JOIN',
    category: 'Outer',
    status: 'implemented',
    description:
      'Returns all rows from right table, with matches from left (NULLs for non-matches)',
    syntax: 'SELECT ... FROM table1 RIGHT JOIN table2 ON condition',
    useCase: 'When you want all records from the second table, even without matches',
  },
  {
    name: 'FULL OUTER JOIN',
    category: 'Outer',
    status: 'implemented',
    description: 'Returns all rows from both tables (NULLs where no match)',
    syntax: 'SELECT ... FROM table1 FULL OUTER JOIN table2 ON condition',
    useCase: 'When you want all records from both tables, showing all matches and non-matches',
  },

  // Cross Join
  {
    name: 'CROSS JOIN',
    category: 'Cross',
    status: 'implemented',
    description: 'Cartesian product - every row from table1 paired with every row from table2',
    syntax: 'SELECT ... FROM table1 CROSS JOIN table2',
    useCase: 'When you need all possible combinations of rows from two tables',
  },

  // Multi-table Joins
  {
    name: 'Multi-table JOIN',
    category: 'Multi',
    status: 'implemented',
    description: 'Join three or more tables together',
    syntax: 'SELECT ... FROM t1 JOIN t2 ON ... JOIN t3 ON ...',
    useCase: 'When data is normalized across multiple related tables',
  },
]

export const JOIN_EXAMPLES = {
  sampleData: `CREATE TABLE departments (
    dept_id INTEGER,
    dept_name VARCHAR(50)
);

CREATE TABLE employees (
    emp_id INTEGER,
    name VARCHAR(50),
    dept_id INTEGER,
    salary INTEGER
);

INSERT INTO departments VALUES (1, 'Engineering');
INSERT INTO departments VALUES (2, 'Sales');
INSERT INTO departments VALUES (3, 'Marketing');

INSERT INTO employees VALUES (101, 'Alice', 1, 95000);
INSERT INTO employees VALUES (102, 'Bob', 2, 75000);
INSERT INTO employees VALUES (103, 'Charlie', 1, 105000);
INSERT INTO employees VALUES (104, 'Diana', NULL, 82000);`,

  innerJoin: `-- Returns only matching rows from both tables
SELECT e.name, d.dept_name, e.salary
FROM employees e
INNER JOIN departments d ON e.dept_id = d.dept_id;

-- Expected: Alice, Bob, Charlie (Diana excluded - no dept)`,

  leftJoin: `-- Returns all employees, with department info where available
SELECT e.name, d.dept_name, e.salary
FROM employees e
LEFT JOIN departments d ON e.dept_id = d.dept_id;

-- Expected: All employees including Diana (dept_name = NULL for Diana)`,

  rightJoin: `-- Returns all departments, with employee info where available
SELECT e.name, d.dept_name
FROM employees e
RIGHT JOIN departments d ON e.dept_id = d.dept_id;

-- Expected: All departments including Marketing (no employees yet)`,

  fullJoin: `-- Returns all rows from both tables
SELECT e.name, d.dept_name
FROM employees e
FULL OUTER JOIN departments d ON e.dept_id = d.dept_id;

-- Expected: All employees AND all departments (NULLs where no match)`,

  crossJoin: `-- Cartesian product
SELECT e.name, d.dept_name
FROM employees e
CROSS JOIN departments d;

-- Expected: 4 employees × 3 departments = 12 rows`,

  multiTable: `CREATE TABLE projects (
    project_id INTEGER,
    project_name VARCHAR(50),
    dept_id INTEGER
);

INSERT INTO projects VALUES (1, 'Website Redesign', 1);
INSERT INTO projects VALUES (2, 'Sales Campaign', 2);

-- Three-table join
SELECT e.name, d.dept_name, p.project_name
FROM employees e
JOIN departments d ON e.dept_id = d.dept_id
JOIN projects p ON d.dept_id = p.dept_id;

-- Shows employees with their departments and projects`,
}

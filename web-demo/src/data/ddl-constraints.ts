/**
 * SQL:1999 DDL and Constraints Examples
 *
 * Demonstrates CREATE TABLE with various constraint types including:
 * - PRIMARY KEY constraints
 * - UNIQUE constraints
 * - CHECK constraints
 * - NOT NULL constraints
 * - Constraint violation examples
 */

export interface DDLExample {
  id: string
  title: string
  database: 'empty'
  sql: string
  description: string
  sqlFeatures: string[]
  expectedOutcome?: 'success' | 'error'
  errorType?: string
}

export const DDL_EXAMPLES: DDLExample[] = [
  // Basic CREATE TABLE
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
    expectedOutcome: 'success',
  },

  // PRIMARY KEY constraint
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
    expectedOutcome: 'success',
  },

  // PRIMARY KEY violation
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
    expectedOutcome: 'error',
    errorType: 'PRIMARY KEY constraint violated',
  },

  // UNIQUE constraint
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
    expectedOutcome: 'success',
  },

  // UNIQUE constraint violation
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
    expectedOutcome: 'error',
    errorType: 'UNIQUE constraint violated',
  },

  // CHECK constraint
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
    expectedOutcome: 'success',
  },

  // CHECK constraint violation
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
    expectedOutcome: 'error',
    errorType: 'CHECK constraint violated',
  },

  // Multiple constraints
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
    expectedOutcome: 'success',
  },

  // NOT NULL enforcement
  {
    id: 'ddl-9',
    title: 'NOT NULL Constraint',
    database: 'empty',
    sql: `CREATE TABLE orders (
    order_id INTEGER PRIMARY KEY,
    customer_name VARCHAR(100),
    total FLOAT CHECK (total > 0)
);

INSERT INTO orders VALUES (1, 'Alice Johnson', 150.00);
INSERT INTO orders VALUES (2, 'Bob Smith', 275.50);

SELECT * FROM orders ORDER BY order_id;`,
    description: 'NOT NULL is enforced on PRIMARY KEY columns automatically',
    sqlFeatures: ['PRIMARY KEY', 'NOT NULL', 'Implicit Constraints'],
    expectedOutcome: 'success',
  },

  // Complex CHECK constraint
  {
    id: 'ddl-10',
    title: 'Complex CHECK Constraint',
    database: 'empty',
    sql: `CREATE TABLE inventory (
    item_id INTEGER PRIMARY KEY,
    name VARCHAR(100),
    quantity INTEGER CHECK (quantity >= 0),
    price FLOAT CHECK (price > 0),
    total_value FLOAT CHECK (total_value = quantity * price)
);

INSERT INTO inventory VALUES (1, 'Widget', 100, 10.0, 1000.0);
INSERT INTO inventory VALUES (2, 'Gadget', 50, 25.0, 1250.0);

SELECT * FROM inventory ORDER BY item_id;`,
    description: 'CHECK constraint with complex expression involving multiple columns',
    sqlFeatures: ['CHECK', 'Expressions', 'Multi-column Constraints'],
    expectedOutcome: 'success',
  },

  // UPDATE with constraint enforcement
  {
    id: 'ddl-11',
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
    expectedOutcome: 'success',
  },

  // UPDATE constraint violation
  {
    id: 'ddl-12',
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
    expectedOutcome: 'error',
    errorType: 'CHECK constraint violated',
  },

  // Composite PRIMARY KEY
  {
    id: 'ddl-13',
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
    expectedOutcome: 'success',
  },

  // Real-world example
  {
    id: 'ddl-14',
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
    expectedOutcome: 'success',
  },
]

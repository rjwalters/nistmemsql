/**
 * Sample Database Definitions
 *
 * Pre-configured sample databases for Core SQL:1999 feature demonstration.
 * Each database includes schema definitions and sample data initialization.
 */

import type { Database } from '../db/types'

export interface SampleTable {
  name: string
  createSql: string
  insertSql: string[]
}

export interface SampleDatabase {
  id: string
  name: string
  description: string
  tables: SampleTable[]
}

/**
 * Employees Sample Database
 * Purpose: Basic DML, JOINs, simple aggregates
 */
export const employeesDatabase: SampleDatabase = {
  id: 'employees',
  name: 'Employees',
  description: 'Sample employee data for basic queries and JOINs',
  tables: [
    {
      name: 'employees',
      createSql: `CREATE TABLE employees (
        id INTEGER,
        name VARCHAR(50),
        department VARCHAR(50),
        salary INTEGER
      );`,
      insertSql: [
        "INSERT INTO employees VALUES (1, 'Alice Johnson', 'Engineering', 95000);",
        "INSERT INTO employees VALUES (2, 'Bob Smith', 'Engineering', 87000);",
        "INSERT INTO employees VALUES (3, 'Carol White', 'Sales', 72000);",
        "INSERT INTO employees VALUES (4, 'David Brown', 'Sales', 68000);",
        "INSERT INTO employees VALUES (5, 'Eve Martinez', 'HR', 65000);",
        "INSERT INTO employees VALUES (6, 'Frank Wilson', 'Engineering', 92000);",
      ],
    },
  ],
}

/**
 * Northwind Sample Database (Simplified)
 * Purpose: Complex JOINs, aggregates, subqueries
 */
export const northwindDatabase: SampleDatabase = {
  id: 'northwind',
  name: 'Northwind',
  description: 'E-commerce database with products, orders, and customers',
  tables: [
    {
      name: 'customers',
      createSql: `CREATE TABLE customers (
        customer_id INTEGER,
        company_name VARCHAR(100),
        contact_name VARCHAR(50),
        city VARCHAR(50),
        country VARCHAR(50)
      );`,
      insertSql: [
        "INSERT INTO customers VALUES (1, 'Alfreds Futterkiste', 'Maria Anders', 'Berlin', 'Germany');",
        "INSERT INTO customers VALUES (2, 'Ana Trujillo', 'Ana Trujillo', 'México D.F.', 'Mexico');",
        "INSERT INTO customers VALUES (3, 'Antonio Moreno', 'Antonio Moreno', 'México D.F.', 'Mexico');",
        "INSERT INTO customers VALUES (4, 'Around the Horn', 'Thomas Hardy', 'London', 'UK');",
        "INSERT INTO customers VALUES (5, 'Berglunds snabbköp', 'Christina Berglund', 'Luleå', 'Sweden');",
      ],
    },
    {
      name: 'products',
      createSql: `CREATE TABLE products (
        product_id INTEGER,
        product_name VARCHAR(100),
        category VARCHAR(50),
        unit_price FLOAT,
        units_in_stock INTEGER
      );`,
      insertSql: [
        "INSERT INTO products VALUES (1, 'Chai', 'Beverages', 18.0, 39);",
        "INSERT INTO products VALUES (2, 'Chang', 'Beverages', 19.0, 17);",
        "INSERT INTO products VALUES (3, 'Aniseed Syrup', 'Condiments', 10.0, 13);",
        "INSERT INTO products VALUES (4, 'Olive Oil', 'Condiments', 21.35, 53);",
        "INSERT INTO products VALUES (5, 'Ikura', 'Seafood', 31.0, 31);",
        "INSERT INTO products VALUES (6, 'Tofu', 'Produce', 23.25, 35);",
        "INSERT INTO products VALUES (7, 'Pavlova', 'Confections', 17.45, 29);",
        "INSERT INTO products VALUES (8, 'Carnarvon Tigers', 'Seafood', 62.5, 42);",
      ],
    },
    {
      name: 'orders',
      createSql: `CREATE TABLE orders (
        order_id INTEGER,
        customer_id INTEGER,
        order_date VARCHAR(20),
        total_amount FLOAT
      );`,
      insertSql: [
        "INSERT INTO orders VALUES (1, 1, '2025-01-15', 145.50);",
        "INSERT INTO orders VALUES (2, 2, '2025-01-16', 89.00);",
        "INSERT INTO orders VALUES (3, 1, '2025-01-17', 220.75);",
        "INSERT INTO orders VALUES (4, 3, '2025-01-18', 156.30);",
        "INSERT INTO orders VALUES (5, 4, '2025-01-19', 98.00);",
        "INSERT INTO orders VALUES (6, 2, '2025-01-20', 305.25);",
      ],
    },
  ],
}

/**
 * Company Sample Database
 * Purpose: Multi-table JOINs and business analytics
 */
export const companyDatabase: SampleDatabase = {
  id: 'company',
  name: 'Company',
  description: 'Corporate database with departments, employees, and projects',
  tables: [
    {
      name: 'departments',
      createSql: `CREATE TABLE departments (
        dept_id INTEGER,
        dept_name VARCHAR(100),
        location VARCHAR(100)
      );`,
      insertSql: [
        "INSERT INTO departments VALUES (1, 'Engineering', 'San Francisco');",
        "INSERT INTO departments VALUES (2, 'Sales', 'New York');",
        "INSERT INTO departments VALUES (3, 'Marketing', 'Los Angeles');",
        "INSERT INTO departments VALUES (4, 'Human Resources', 'Chicago');",
        "INSERT INTO departments VALUES (5, 'Operations', 'Seattle');",
      ],
    },
    {
      name: 'employees',
      createSql: `CREATE TABLE employees (
        emp_id INTEGER,
        name VARCHAR(100),
        dept_id INTEGER,
        salary INTEGER,
        hire_date VARCHAR(20)
      );`,
      insertSql: [
        "INSERT INTO employees VALUES (1, 'Alice Chen', 1, 125000, '2020-01-15');",
        "INSERT INTO employees VALUES (2, 'Bob Martinez', 1, 110000, '2021-03-22');",
        "INSERT INTO employees VALUES (3, 'Carol Williams', 1, 95000, '2022-06-10');",
        "INSERT INTO employees VALUES (4, 'David Thompson', 2, 85000, '2019-11-05');",
        "INSERT INTO employees VALUES (5, 'Emma Davis', 2, 92000, '2020-08-17');",
        "INSERT INTO employees VALUES (6, 'Frank Lee', 2, 78000, '2023-02-28');",
        "INSERT INTO employees VALUES (7, 'Grace Park', 3, 88000, '2021-04-12');",
        "INSERT INTO employees VALUES (8, 'Henry Wilson', 3, 82000, '2022-01-20');",
        "INSERT INTO employees VALUES (9, 'Iris Brown', 4, 75000, '2020-09-15');",
        "INSERT INTO employees VALUES (10, 'Jack Robinson', 4, 71000, '2022-11-08');",
        "INSERT INTO employees VALUES (11, 'Karen Miller', 5, 68000, '2021-07-03');",
        "INSERT INTO employees VALUES (12, 'Leo Garcia', 5, 72000, '2022-05-18');",
        "INSERT INTO employees VALUES (13, 'Maria Rodriguez', 1, 118000, '2019-02-10');",
        "INSERT INTO employees VALUES (14, 'Nathan Taylor', 2, 89000, '2020-12-01');",
        "INSERT INTO employees VALUES (15, 'Olivia Johnson', 3, 91000, '2021-10-25');",
        "INSERT INTO employees VALUES (16, 'Paul Anderson', 5, 70000, '2023-03-14');",
        "INSERT INTO employees VALUES (17, 'Quinn Moore', NULL, 95000, '2023-01-09');",
        "INSERT INTO employees VALUES (18, 'Rachel White', NULL, 88000, '2023-04-20');",
        "INSERT INTO employees VALUES (19, 'Sam Harris', 1, NULL, '2023-06-05');",
        "INSERT INTO employees VALUES (20, 'Tina Clark', 2, NULL, '2023-07-12');",
      ],
    },
    {
      name: 'projects',
      createSql: `CREATE TABLE projects (
        project_id INTEGER,
        project_name VARCHAR(100),
        dept_id INTEGER,
        budget INTEGER
      );`,
      insertSql: [
        "INSERT INTO projects VALUES (1, 'Cloud Migration', 1, 500000);",
        "INSERT INTO projects VALUES (2, 'Mobile App Redesign', 1, 250000);",
        "INSERT INTO projects VALUES (3, 'Q4 Sales Campaign', 2, 150000);",
        "INSERT INTO projects VALUES (4, 'Customer Portal', 1, 320000);",
        "INSERT INTO projects VALUES (5, 'Brand Refresh', 3, 180000);",
        "INSERT INTO projects VALUES (6, 'Market Research Initiative', 3, 95000);",
        "INSERT INTO projects VALUES (7, 'Employee Wellness Program', 4, 75000);",
        "INSERT INTO projects VALUES (8, 'Supply Chain Optimization', 5, 410000);",
        "INSERT INTO projects VALUES (9, 'Innovation Lab', NULL, 200000);",
        "INSERT INTO projects VALUES (10, 'Security Audit', NULL, NULL);",
      ],
    },
  ],
}

/**
 * Empty Database
 * Purpose: Clean slate for user experimentation
 */
export const emptyDatabase: SampleDatabase = {
  id: 'empty',
  name: 'Empty',
  description: 'Start with a blank database',
  tables: [],
}

/**
 * All available sample databases
 */
export const sampleDatabases = [
  employeesDatabase,
  northwindDatabase,
  companyDatabase,
  emptyDatabase,
]

/**
 * Load a sample database into the WASM database instance
 *
 * @param database - WASM database instance
 * @param sampleDb - Sample database to load
 * @returns Success or error
 */
export function loadSampleDatabase(database: Database, sampleDb: SampleDatabase): void {
  // Drop all existing tables first
  const existingTables = database.list_tables()
  for (const table of existingTables) {
    try {
      database.execute(`DROP TABLE ${table};`)
    } catch (error) {
      console.warn(`Failed to drop table ${table}:`, error)
    }
  }

  // Create tables and insert data
  for (const table of sampleDb.tables) {
    // Create table
    database.execute(table.createSql)

    // Insert rows
    for (const insertSql of table.insertSql) {
      database.execute(insertSql)
    }
  }
}

/**
 * Get sample database by ID
 *
 * @param id - Database ID
 * @returns Sample database or undefined
 */
export function getSampleDatabase(id: string): SampleDatabase | undefined {
  return sampleDatabases.find(db => db.id === id)
}

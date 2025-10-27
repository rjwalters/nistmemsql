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
export const sampleDatabases = [employeesDatabase, northwindDatabase, emptyDatabase]

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

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
 * University Sample Database
 * Purpose: Complex relationships, correlated subqueries, aggregates
 */
export const universityDatabase: SampleDatabase = {
  id: 'university',
  name: 'University',
  description: 'Academic database with students, courses, enrollments, and professors',
  tables: [
    {
      name: 'students',
      createSql: `CREATE TABLE students (
        student_id INTEGER,
        name VARCHAR(100),
        major VARCHAR(50),
        gpa FLOAT
      );`,
      insertSql: [
        // Computer Science majors
        "INSERT INTO students VALUES (1001, 'Emma Wilson', 'Computer Science', 3.8);",
        "INSERT INTO students VALUES (1002, 'Liam Chen', 'Computer Science', 3.6);",
        "INSERT INTO students VALUES (1003, 'Olivia Rodriguez', 'Computer Science', 3.9);",
        "INSERT INTO students VALUES (1004, 'Noah Anderson', 'Computer Science', 3.4);",
        "INSERT INTO students VALUES (1005, 'Ava Martinez', 'Computer Science', 3.7);",
        "INSERT INTO students VALUES (1006, 'Ethan Brown', 'Computer Science', 2.9);",
        "INSERT INTO students VALUES (1007, 'Sophia Taylor', 'Computer Science', 3.5);",
        "INSERT INTO students VALUES (1008, 'Mason Garcia', 'Computer Science', NULL);", // Probationary
        // Mathematics majors
        "INSERT INTO students VALUES (1009, 'Isabella Lee', 'Mathematics', 3.9);",
        "INSERT INTO students VALUES (1010, 'James White', 'Mathematics', 3.6);",
        "INSERT INTO students VALUES (1011, 'Charlotte Kim', 'Mathematics', 3.8);",
        "INSERT INTO students VALUES (1012, 'Benjamin Clark', 'Mathematics', 3.3);",
        "INSERT INTO students VALUES (1013, 'Amelia Lewis', 'Mathematics', 3.7);",
        "INSERT INTO students VALUES (1014, 'Lucas Hall', 'Mathematics', 2.8);",
        "INSERT INTO students VALUES (1015, 'Mia Young', 'Mathematics', 3.4);",
        // Physics majors
        "INSERT INTO students VALUES (1016, 'Henry Allen', 'Physics', 3.5);",
        "INSERT INTO students VALUES (1017, 'Harper Scott', 'Physics', 3.9);",
        "INSERT INTO students VALUES (1018, 'Alexander King', 'Physics', 3.2);",
        "INSERT INTO students VALUES (1019, 'Evelyn Wright', 'Physics', 3.6);",
        "INSERT INTO students VALUES (1020, 'Daniel Green', 'Physics', 3.1);",
        "INSERT INTO students VALUES (1021, 'Abigail Hill', 'Physics', 3.7);",
        "INSERT INTO students VALUES (1022, 'Michael Adams', 'Physics', NULL);", // Probationary
        // Biology majors
        "INSERT INTO students VALUES (1023, 'Emily Baker', 'Biology', 3.4);",
        "INSERT INTO students VALUES (1024, 'William Nelson', 'Biology', 3.8);",
        "INSERT INTO students VALUES (1025, 'Madison Mitchell', 'Biology', 3.2);",
        "INSERT INTO students VALUES (1026, 'Jacob Turner', 'Biology', 3.6);",
        "INSERT INTO students VALUES (1027, 'Ella Parker', 'Biology', 3.9);",
        "INSERT INTO students VALUES (1028, 'Joseph Edwards', 'Biology', 2.7);",
        "INSERT INTO students VALUES (1029, 'Avery Collins', 'Biology', 3.5);",
        // English majors
        "INSERT INTO students VALUES (1030, 'David Stewart', 'English', 3.3);",
        "INSERT INTO students VALUES (1031, 'Sofia Morris', 'English', 3.7);",
        "INSERT INTO students VALUES (1032, 'Matthew Rogers', 'English', 3.1);",
        "INSERT INTO students VALUES (1033, 'Grace Reed', 'English', 3.8);",
        "INSERT INTO students VALUES (1034, 'Samuel Cook', 'English', 2.9);",
        "INSERT INTO students VALUES (1035, 'Chloe Morgan', 'English', 3.4);",
        "INSERT INTO students VALUES (1036, 'Christopher Bell', 'English', 3.6);",
        // History majors
        "INSERT INTO students VALUES (1037, 'Victoria Murphy', 'History', 3.2);",
        "INSERT INTO students VALUES (1038, 'Andrew Bailey', 'History', 3.5);",
        "INSERT INTO students VALUES (1039, 'Zoe Rivera', 'History', 3.9);",
        "INSERT INTO students VALUES (1040, 'Joshua Cooper', 'History', 3.0);",
        "INSERT INTO students VALUES (1041, 'Lily Richardson', 'History', 3.7);",
        "INSERT INTO students VALUES (1042, 'Ryan Cox', 'History', 2.6);",
        "INSERT INTO students VALUES (1043, 'Hannah Howard', 'History', 3.4);",
        "INSERT INTO students VALUES (1044, 'Nathan Ward', 'History', 3.8);",
        // Mixed majors (remaining)
        "INSERT INTO students VALUES (1045, 'Aubrey Torres', 'Computer Science', 3.2);",
        "INSERT INTO students VALUES (1046, 'Sebastian Peterson', 'Mathematics', 3.5);",
        "INSERT INTO students VALUES (1047, 'Zoey Gray', 'Physics', 3.6);",
        "INSERT INTO students VALUES (1048, 'Jack Ramirez', 'Biology', 3.3);",
        "INSERT INTO students VALUES (1049, 'Layla James', 'English', 3.8);",
        "INSERT INTO students VALUES (1050, 'Owen Watson', 'History', 3.1);",
      ],
    },
    {
      name: 'courses',
      createSql: `CREATE TABLE courses (
        course_id INTEGER,
        course_name VARCHAR(100),
        department VARCHAR(50),
        credits INTEGER
      );`,
      insertSql: [
        // Computer Science courses
        "INSERT INTO courses VALUES (101, 'Intro to Programming', 'CS', 3);",
        "INSERT INTO courses VALUES (102, 'Data Structures', 'CS', 4);",
        "INSERT INTO courses VALUES (103, 'Database Systems', 'CS', 3);",
        "INSERT INTO courses VALUES (104, 'Operating Systems', 'CS', 4);",
        // Mathematics courses
        "INSERT INTO courses VALUES (105, 'Calculus I', 'MATH', 4);",
        "INSERT INTO courses VALUES (106, 'Calculus II', 'MATH', 4);",
        "INSERT INTO courses VALUES (107, 'Linear Algebra', 'MATH', 3);",
        "INSERT INTO courses VALUES (108, 'Discrete Mathematics', 'MATH', 3);",
        // Physics courses
        "INSERT INTO courses VALUES (109, 'General Physics I', 'PHYS', 4);",
        "INSERT INTO courses VALUES (110, 'General Physics II', 'PHYS', 4);",
        "INSERT INTO courses VALUES (111, 'Modern Physics', 'PHYS', 3);",
        // Biology courses
        "INSERT INTO courses VALUES (112, 'General Biology', 'BIO', 4);",
        "INSERT INTO courses VALUES (113, 'Molecular Biology', 'BIO', 3);",
        "INSERT INTO courses VALUES (114, 'Genetics', 'BIO', 3);",
        // English courses
        "INSERT INTO courses VALUES (115, 'English Composition', 'ENG', 3);",
        "INSERT INTO courses VALUES (116, 'American Literature', 'ENG', 3);",
        "INSERT INTO courses VALUES (117, 'Creative Writing', 'ENG', 3);",
        // History courses
        "INSERT INTO courses VALUES (118, 'World History I', 'HIST', 3);",
        "INSERT INTO courses VALUES (119, 'World History II', 'HIST', 3);",
        "INSERT INTO courses VALUES (120, 'US History', 'HIST', 3);",
      ],
    },
    {
      name: 'enrollments',
      createSql: `CREATE TABLE enrollments (
        student_id INTEGER,
        course_id INTEGER,
        semester VARCHAR(20),
        grade VARCHAR(2)
      );`,
      insertSql: [
        // Fall 2024 enrollments
        "INSERT INTO enrollments VALUES (1001, 101, 'Fall2024', 'A');",
        "INSERT INTO enrollments VALUES (1001, 105, 'Fall2024', 'B');",
        "INSERT INTO enrollments VALUES (1002, 101, 'Fall2024', 'B');",
        "INSERT INTO enrollments VALUES (1002, 107, 'Fall2024', 'A');",
        "INSERT INTO enrollments VALUES (1003, 102, 'Fall2024', 'A');",
        "INSERT INTO enrollments VALUES (1003, 106, 'Fall2024', 'A');",
        "INSERT INTO enrollments VALUES (1004, 101, 'Fall2024', 'C');",
        "INSERT INTO enrollments VALUES (1004, 105, 'Fall2024', 'B');",
        "INSERT INTO enrollments VALUES (1005, 102, 'Fall2024', 'B');",
        "INSERT INTO enrollments VALUES (1005, 108, 'Fall2024', 'A');",
        "INSERT INTO enrollments VALUES (1006, 101, 'Fall2024', 'D');",
        "INSERT INTO enrollments VALUES (1006, 115, 'Fall2024', 'C');",
        "INSERT INTO enrollments VALUES (1007, 103, 'Fall2024', 'B');",
        "INSERT INTO enrollments VALUES (1007, 107, 'Fall2024', 'B');",
        "INSERT INTO enrollments VALUES (1009, 105, 'Fall2024', 'A');",
        "INSERT INTO enrollments VALUES (1009, 107, 'Fall2024', 'A');",
        "INSERT INTO enrollments VALUES (1010, 106, 'Fall2024', 'B');",
        "INSERT INTO enrollments VALUES (1010, 108, 'Fall2024', 'A');",
        "INSERT INTO enrollments VALUES (1011, 105, 'Fall2024', 'A');",
        "INSERT INTO enrollments VALUES (1011, 115, 'Fall2024', 'B');",
        "INSERT INTO enrollments VALUES (1012, 105, 'Fall2024', 'C');",
        "INSERT INTO enrollments VALUES (1012, 118, 'Fall2024', 'B');",
        "INSERT INTO enrollments VALUES (1013, 106, 'Fall2024', 'B');",
        "INSERT INTO enrollments VALUES (1013, 107, 'Fall2024', 'A');",
        "INSERT INTO enrollments VALUES (1014, 105, 'Fall2024', 'D');",
        "INSERT INTO enrollments VALUES (1014, 115, 'Fall2024', 'C');",
        "INSERT INTO enrollments VALUES (1015, 108, 'Fall2024', 'C');",
        "INSERT INTO enrollments VALUES (1015, 116, 'Fall2024', 'B');",
        "INSERT INTO enrollments VALUES (1016, 109, 'Fall2024', 'B');",
        "INSERT INTO enrollments VALUES (1016, 105, 'Fall2024', 'C');",
        "INSERT INTO enrollments VALUES (1017, 109, 'Fall2024', 'A');",
        "INSERT INTO enrollments VALUES (1017, 106, 'Fall2024', 'A');",
        "INSERT INTO enrollments VALUES (1018, 109, 'Fall2024', 'C');",
        "INSERT INTO enrollments VALUES (1018, 115, 'Fall2024', 'D');",
        "INSERT INTO enrollments VALUES (1019, 110, 'Fall2024', 'B');",
        "INSERT INTO enrollments VALUES (1019, 107, 'Fall2024', 'A');",
        "INSERT INTO enrollments VALUES (1020, 109, 'Fall2024', 'C');",
        "INSERT INTO enrollments VALUES (1020, 118, 'Fall2024', 'C');",
        "INSERT INTO enrollments VALUES (1021, 110, 'Fall2024', 'B');",
        "INSERT INTO enrollments VALUES (1021, 106, 'Fall2024', 'A');",
        "INSERT INTO enrollments VALUES (1023, 112, 'Fall2024', 'C');",
        "INSERT INTO enrollments VALUES (1023, 105, 'Fall2024', 'B');",
        "INSERT INTO enrollments VALUES (1024, 112, 'Fall2024', 'A');",
        "INSERT INTO enrollments VALUES (1024, 106, 'Fall2024', 'A');",
        "INSERT INTO enrollments VALUES (1025, 112, 'Fall2024', 'C');",
        "INSERT INTO enrollments VALUES (1025, 115, 'Fall2024', 'D');",
        "INSERT INTO enrollments VALUES (1026, 113, 'Fall2024', 'B');",
        "INSERT INTO enrollments VALUES (1026, 107, 'Fall2024', 'A');",
        "INSERT INTO enrollments VALUES (1027, 112, 'Fall2024', 'A');",
        "INSERT INTO enrollments VALUES (1027, 106, 'Fall2024', 'A');",
        "INSERT INTO enrollments VALUES (1028, 112, 'Fall2024', 'D');",
        "INSERT INTO enrollments VALUES (1028, 115, 'Fall2024', 'F');",
        "INSERT INTO enrollments VALUES (1029, 113, 'Fall2024', 'B');",
        "INSERT INTO enrollments VALUES (1029, 108, 'Fall2024', 'C');",
        // Spring 2025 enrollments
        "INSERT INTO enrollments VALUES (1030, 115, 'Spring2025', 'C');",
        "INSERT INTO enrollments VALUES (1030, 118, 'Spring2025', 'B');",
        "INSERT INTO enrollments VALUES (1031, 116, 'Spring2025', 'B');",
        "INSERT INTO enrollments VALUES (1031, 119, 'Spring2025', 'A');",
        "INSERT INTO enrollments VALUES (1032, 115, 'Spring2025', 'C');",
        "INSERT INTO enrollments VALUES (1032, 105, 'Spring2025', 'D');",
        "INSERT INTO enrollments VALUES (1033, 117, 'Spring2025', 'A');",
        "INSERT INTO enrollments VALUES (1033, 119, 'Spring2025', 'A');",
        "INSERT INTO enrollments VALUES (1034, 115, 'Spring2025', 'D');",
        "INSERT INTO enrollments VALUES (1034, 118, 'Spring2025', 'C');",
        "INSERT INTO enrollments VALUES (1035, 116, 'Spring2025', 'C');",
        "INSERT INTO enrollments VALUES (1035, 107, 'Spring2025', 'B');",
        "INSERT INTO enrollments VALUES (1036, 117, 'Spring2025', 'B');",
        "INSERT INTO enrollments VALUES (1036, 120, 'Spring2025', 'A');",
        "INSERT INTO enrollments VALUES (1037, 118, 'Spring2025', 'C');",
        "INSERT INTO enrollments VALUES (1037, 105, 'Spring2025', 'D');",
        "INSERT INTO enrollments VALUES (1038, 119, 'Spring2025', 'B');",
        "INSERT INTO enrollments VALUES (1038, 106, 'Spring2025', 'C');",
        "INSERT INTO enrollments VALUES (1039, 120, 'Spring2025', 'A');",
        "INSERT INTO enrollments VALUES (1039, 107, 'Spring2025', 'A');",
        "INSERT INTO enrollments VALUES (1040, 118, 'Spring2025', 'C');",
        "INSERT INTO enrollments VALUES (1040, 115, 'Spring2025', 'D');",
        "INSERT INTO enrollments VALUES (1041, 119, 'Spring2025', 'B');",
        "INSERT INTO enrollments VALUES (1041, 116, 'Spring2025', 'A');",
        "INSERT INTO enrollments VALUES (1042, 118, 'Spring2025', 'F');",
        "INSERT INTO enrollments VALUES (1042, 115, 'Spring2025', 'D');",
        "INSERT INTO enrollments VALUES (1043, 120, 'Spring2025', 'C');",
        "INSERT INTO enrollments VALUES (1043, 117, 'Spring2025', 'B');",
        "INSERT INTO enrollments VALUES (1044, 119, 'Spring2025', 'A');",
        "INSERT INTO enrollments VALUES (1044, 116, 'Spring2025', 'A');",
        // Fall 2025 enrollments (some in progress with NULL grades)
        "INSERT INTO enrollments VALUES (1001, 103, 'Fall2025', 'A');",
        "INSERT INTO enrollments VALUES (1001, 109, 'Fall2025', NULL);", // In progress
        "INSERT INTO enrollments VALUES (1002, 104, 'Fall2025', 'B');",
        "INSERT INTO enrollments VALUES (1002, 110, 'Fall2025', NULL);", // In progress
        "INSERT INTO enrollments VALUES (1003, 103, 'Fall2025', NULL);", // In progress
        "INSERT INTO enrollments VALUES (1004, 102, 'Fall2025', 'C');",
        "INSERT INTO enrollments VALUES (1005, 104, 'Fall2025', NULL);", // In progress
        "INSERT INTO enrollments VALUES (1009, 106, 'Fall2025', 'A');",
        "INSERT INTO enrollments VALUES (1010, 107, 'Fall2025', 'B');",
        "INSERT INTO enrollments VALUES (1016, 110, 'Fall2025', 'B');",
        "INSERT INTO enrollments VALUES (1017, 111, 'Fall2025', NULL);", // In progress
        "INSERT INTO enrollments VALUES (1024, 113, 'Fall2025', 'A');",
        "INSERT INTO enrollments VALUES (1027, 114, 'Fall2025', 'A');",
        "INSERT INTO enrollments VALUES (1031, 117, 'Fall2025', 'B');",
        "INSERT INTO enrollments VALUES (1039, 120, 'Fall2025', NULL);", // In progress
        "INSERT INTO enrollments VALUES (1045, 101, 'Fall2025', 'C');",
        "INSERT INTO enrollments VALUES (1046, 108, 'Fall2025', 'B');",
        "INSERT INTO enrollments VALUES (1047, 109, 'Fall2025', 'B');",
        "INSERT INTO enrollments VALUES (1048, 112, 'Fall2025', 'C');",
        "INSERT INTO enrollments VALUES (1049, 116, 'Fall2025', 'A');",
        "INSERT INTO enrollments VALUES (1050, 119, 'Fall2025', NULL);", // In progress
      ],
    },
    {
      name: 'professors',
      createSql: `CREATE TABLE professors (
        prof_id INTEGER,
        name VARCHAR(100),
        department VARCHAR(50),
        salary INTEGER
      );`,
      insertSql: [
        "INSERT INTO professors VALUES (201, 'Dr. Sarah Johnson', 'CS', 95000);",
        "INSERT INTO professors VALUES (202, 'Dr. Robert Chen', 'CS', 98000);",
        "INSERT INTO professors VALUES (203, 'Dr. Maria Garcia', 'MATH', 92000);",
        "INSERT INTO professors VALUES (204, 'Dr. James Wilson', 'MATH', 94000);",
        "INSERT INTO professors VALUES (205, 'Dr. Patricia Brown', 'PHYS', 97000);",
        "INSERT INTO professors VALUES (206, 'Dr. Michael Davis', 'PHYS', NULL);", // Visiting prof
        "INSERT INTO professors VALUES (207, 'Dr. Jennifer Martinez', 'BIO', 90000);",
        "INSERT INTO professors VALUES (208, 'Dr. William Lee', 'ENG', 85000);",
        "INSERT INTO professors VALUES (209, 'Dr. Elizabeth Taylor', 'ENG', 87000);",
        "INSERT INTO professors VALUES (210, 'Dr. David Anderson', 'HIST', NULL);", // Visiting prof
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
export const sampleDatabases = [employeesDatabase, northwindDatabase, universityDatabase, emptyDatabase]

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

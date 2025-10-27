export interface DataType {
  name: string
  status: 'implemented' | 'planned'
  description: string
  exampleValue: string
  sqlSpec: string
}

export const SQL_DATA_TYPES: DataType[] = [
  {
    name: 'INTEGER',
    status: 'implemented',
    description: 'Whole numbers without decimal points',
    exampleValue: '42, -17, 0',
    sqlSpec: 'SQL:1999 Core',
  },
  {
    name: 'VARCHAR',
    status: 'implemented',
    description: 'Variable-length character strings',
    exampleValue: "'Hello', 'SQL:1999'",
    sqlSpec: 'SQL:1999 Core',
  },
  {
    name: 'FLOAT',
    status: 'implemented',
    description: 'Floating-point numbers with approximate precision',
    exampleValue: '3.14159, -0.001, 1.5e10',
    sqlSpec: 'SQL:1999 Core',
  },
  {
    name: 'BOOLEAN',
    status: 'implemented',
    description: 'Logical true or false values',
    exampleValue: 'TRUE, FALSE',
    sqlSpec: 'SQL:1999 Core',
  },
  {
    name: 'NULL',
    status: 'implemented',
    description: 'Represents absence of a value (three-valued logic)',
    exampleValue: 'NULL',
    sqlSpec: 'SQL:1999 Core',
  },
  {
    name: 'NUMERIC/DECIMAL',
    status: 'planned',
    description: 'Fixed-point exact numeric values',
    exampleValue: '123.45, 99.9999',
    sqlSpec: 'SQL:1999 Core',
  },
  {
    name: 'SMALLINT',
    status: 'planned',
    description: 'Small-range integer (-32768 to 32767)',
    exampleValue: '100, -200',
    sqlSpec: 'SQL:1999 Core',
  },
  {
    name: 'BIGINT',
    status: 'planned',
    description: 'Large-range integer',
    exampleValue: '9223372036854775807',
    sqlSpec: 'SQL:1999 Core',
  },
  {
    name: 'REAL',
    status: 'planned',
    description: 'Single-precision floating-point',
    exampleValue: '3.14',
    sqlSpec: 'SQL:1999 Core',
  },
  {
    name: 'DOUBLE PRECISION',
    status: 'planned',
    description: 'Double-precision floating-point',
    exampleValue: '3.141592653589793',
    sqlSpec: 'SQL:1999 Core',
  },
  {
    name: 'CHAR',
    status: 'planned',
    description: 'Fixed-length character strings',
    exampleValue: "'CODE'",
    sqlSpec: 'SQL:1999 Core',
  },
  {
    name: 'DATE',
    status: 'planned',
    description: 'Calendar date (year, month, day)',
    exampleValue: '2025-10-26',
    sqlSpec: 'SQL:1999 Core',
  },
  {
    name: 'TIME',
    status: 'planned',
    description: 'Time of day (hour, minute, second)',
    exampleValue: '14:30:00',
    sqlSpec: 'SQL:1999 Core',
  },
  {
    name: 'TIMESTAMP',
    status: 'planned',
    description: 'Date and time combined',
    exampleValue: '2025-10-26 14:30:00',
    sqlSpec: 'SQL:1999 Core',
  },
]

export const EXAMPLE_QUERIES = {
  numericTypes: `-- Create table with numeric types
CREATE TABLE products (
    id INTEGER,
    name VARCHAR(100),
    price FLOAT,
    in_stock BOOLEAN
);

-- Insert sample data
INSERT INTO products VALUES (1, 'Laptop', 999.99, TRUE);
INSERT INTO products VALUES (2, 'Mouse', 24.99, TRUE);
INSERT INTO products VALUES (3, 'Monitor', NULL, FALSE);

-- Query with type operations
SELECT * FROM products WHERE price > 50.0 AND in_stock = TRUE;`,

  nullHandling: `-- Three-valued logic with NULL
SELECT * FROM products WHERE price IS NULL;
SELECT * FROM products WHERE price IS NOT NULL;

-- NULL in comparisons
SELECT name, price FROM products WHERE price > 0 OR price IS NULL;`,

  typeComparisons: `-- Demonstrate type compatibility
SELECT name, price FROM products WHERE price BETWEEN 20.0 AND 100.0;
SELECT COUNT(*) AS total_products FROM products;

-- Boolean operations
SELECT name, in_stock FROM products WHERE in_stock = TRUE;`,
}

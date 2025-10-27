export interface Predicate {
  name: string
  category: 'Comparison' | 'NULL' | 'Range' | 'Set' | 'Boolean' | 'Planned'
  status: 'implemented' | 'planned'
  description: string
  syntax: string
}

export const PREDICATES: Predicate[] = [
  // Comparison Operators
  {
    name: '=',
    category: 'Comparison',
    status: 'implemented',
    description: 'Equal to',
    syntax: 'expr = expr',
  },
  {
    name: '<>',
    category: 'Comparison',
    status: 'implemented',
    description: 'Not equal to',
    syntax: 'expr <> expr',
  },
  {
    name: '<',
    category: 'Comparison',
    status: 'implemented',
    description: 'Less than',
    syntax: 'expr < expr',
  },
  {
    name: '<=',
    category: 'Comparison',
    status: 'implemented',
    description: 'Less than or equal',
    syntax: 'expr <= expr',
  },
  {
    name: '>',
    category: 'Comparison',
    status: 'implemented',
    description: 'Greater than',
    syntax: 'expr > expr',
  },
  {
    name: '>=',
    category: 'Comparison',
    status: 'implemented',
    description: 'Greater than or equal',
    syntax: 'expr >= expr',
  },

  // NULL Predicates
  {
    name: 'IS NULL',
    category: 'NULL',
    status: 'implemented',
    description: 'Test for NULL value',
    syntax: 'expr IS NULL',
  },
  {
    name: 'IS NOT NULL',
    category: 'NULL',
    status: 'implemented',
    description: 'Test for non-NULL value',
    syntax: 'expr IS NOT NULL',
  },

  // Range Predicates
  {
    name: 'BETWEEN',
    category: 'Range',
    status: 'implemented',
    description: 'Value within range (inclusive)',
    syntax: 'expr BETWEEN low AND high',
  },
  {
    name: 'NOT BETWEEN',
    category: 'Range',
    status: 'implemented',
    description: 'Value outside range',
    syntax: 'expr NOT BETWEEN low AND high',
  },

  // Set Predicates
  {
    name: 'IN (subquery)',
    category: 'Set',
    status: 'implemented',
    description: 'Value in subquery results',
    syntax: 'expr IN (SELECT ...)',
  },

  // Boolean Operators
  {
    name: 'AND',
    category: 'Boolean',
    status: 'implemented',
    description: 'Logical conjunction',
    syntax: 'condition AND condition',
  },
  {
    name: 'OR',
    category: 'Boolean',
    status: 'implemented',
    description: 'Logical disjunction',
    syntax: 'condition OR condition',
  },
  {
    name: 'NOT',
    category: 'Boolean',
    status: 'implemented',
    description: 'Logical negation',
    syntax: 'NOT condition',
  },

  // Planned Features
  {
    name: 'LIKE',
    category: 'Planned',
    status: 'planned',
    description: 'Pattern matching',
    syntax: 'expr LIKE pattern',
  },
  {
    name: 'EXISTS',
    category: 'Planned',
    status: 'planned',
    description: 'Subquery returns rows',
    syntax: 'EXISTS (SELECT ...)',
  },
  {
    name: 'ALL/SOME/ANY',
    category: 'Planned',
    status: 'planned',
    description: 'Quantified comparisons',
    syntax: 'expr = ALL (SELECT ...)',
  },
  {
    name: 'CASE',
    category: 'Planned',
    status: 'planned',
    description: 'Conditional expressions',
    syntax: 'CASE WHEN ... THEN ... END',
  },
  {
    name: 'COALESCE',
    category: 'Planned',
    status: 'planned',
    description: 'First non-NULL value',
    syntax: 'COALESCE(expr, ...)',
  },
]

export const PREDICATE_EXAMPLES = {
  comparison: `CREATE TABLE inventory (
    product VARCHAR(50),
    quantity INTEGER,
    price FLOAT
);

INSERT INTO inventory VALUES ('Laptop', 5, 999.99);
INSERT INTO inventory VALUES ('Mouse', 50, 19.99);
INSERT INTO inventory VALUES ('Keyboard', 30, 79.99);

-- Comparison operators
SELECT * FROM inventory WHERE quantity > 10;
SELECT * FROM inventory WHERE price <= 50.0;
SELECT * FROM inventory WHERE product = 'Laptop';
SELECT * FROM inventory WHERE quantity <> 50;`,

  between: `-- BETWEEN (inclusive range)
SELECT product FROM inventory WHERE quantity BETWEEN 10 AND 40;

-- NOT BETWEEN
SELECT product FROM inventory WHERE price NOT BETWEEN 50.0 AND 100.0;

-- BETWEEN with expressions
SELECT * FROM inventory WHERE price BETWEEN 10.0 AND 100.0;`,

  nullPredicates: `-- Insert row with NULL
INSERT INTO inventory VALUES ('Monitor', NULL, 299.99);

-- IS NULL test
SELECT * FROM inventory WHERE quantity IS NULL;

-- IS NOT NULL test
SELECT * FROM inventory WHERE quantity IS NOT NULL;

-- Three-valued logic
SELECT * FROM inventory WHERE quantity > 20 OR quantity IS NULL;`,

  booleanLogic: `-- AND (both must be TRUE)
SELECT * FROM inventory WHERE quantity > 20 AND price < 100.0;

-- OR (at least one must be TRUE)
SELECT * FROM inventory WHERE quantity < 10 OR price > 500.0;

-- NOT (logical negation)
SELECT * FROM inventory WHERE NOT (quantity < 20);

-- Complex combinations
SELECT * FROM inventory
WHERE (quantity > 10 AND price < 100.0) OR product = 'Laptop';`,

  inPredicate: `CREATE TABLE popular_products (name VARCHAR(50));
INSERT INTO popular_products VALUES ('Laptop');
INSERT INTO popular_products VALUES ('Mouse');

-- IN with subquery
SELECT * FROM inventory
WHERE product IN (SELECT name FROM popular_products);

-- NOT IN
SELECT * FROM inventory
WHERE product NOT IN (SELECT name FROM popular_products);`,

  combined: `-- Complex predicate combinations
SELECT product, quantity, price
FROM inventory
WHERE (quantity BETWEEN 20 AND 60
   OR price < 50.0)
  AND quantity IS NOT NULL
  AND product IN (SELECT name FROM popular_products)
ORDER BY price DESC;`,
}

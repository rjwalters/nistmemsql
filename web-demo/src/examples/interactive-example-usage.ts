/**
 * Interactive Example Framework - Usage Examples
 *
 * This file demonstrates how to use the InteractiveExample component
 * for feature showcases in the web demo.
 */

import {
  InteractiveExample,
  type InteractiveExampleConfig,
  type InteractiveExampleCallbacks,
} from '../components/InteractiveExample'

// Example 1: Basic SELECT query
export function createBasicSelectExample(): InteractiveExample {
  const config: InteractiveExampleConfig = {
    title: 'Basic SELECT Query',
    description: 'Simple SELECT statement to retrieve all rows from a table',
    initialQuery: 'SELECT * FROM users;',
    expectedResults: {
      columns: ['id', 'name', 'email'],
      rows: [
        [1, 'Alice', 'alice@example.com'],
        [2, 'Bob', 'bob@example.com'],
      ],
    },
    database: 'empty',
  }

  const callbacks: InteractiveExampleCallbacks = {
    onRunQuery: async (_query, _database) => {
      // In real usage, this would call the WASM executor
      // For this example, return mock data
      return {
        columns: ['id', 'name', 'email'],
        rows: [
          [1, 'Alice', 'alice@example.com'],
          [2, 'Bob', 'bob@example.com'],
        ],
      }
    },
  }

  return new InteractiveExample('example-container-1', config, callbacks)
}

// Example 2: Aggregate query
export function createAggregateExample(): InteractiveExample {
  const config: InteractiveExampleConfig = {
    title: 'Aggregate Functions',
    description: 'Using COUNT, SUM, and AVG to compute statistics',
    initialQuery: `SELECT
  department,
  COUNT(*) AS employee_count,
  AVG(salary) AS avg_salary
FROM employees
GROUP BY department;`,
    expectedResults: {
      columns: ['department', 'employee_count', 'avg_salary'],
      rows: [
        ['Engineering', 10, 95000],
        ['Sales', 8, 75000],
      ],
    },
    database: 'employees',
  }

  const callbacks: InteractiveExampleCallbacks = {
    onRunQuery: async (_query, _database) => {
      // Execute via WASM
      return {
        columns: ['department', 'employee_count', 'avg_salary'],
        rows: [
          ['Engineering', 10, 95000],
          ['Sales', 8, 75000],
        ],
      }
    },
    onLoadDatabase: async _database => {
      // Load sample database via WASM
    },
  }

  return new InteractiveExample('example-container-2', config, callbacks)
}

// Example 3: JOIN query
export function createJoinExample(): InteractiveExample {
  const config: InteractiveExampleConfig = {
    title: 'INNER JOIN',
    description: 'Combining data from two tables using INNER JOIN',
    initialQuery: `SELECT
  orders.order_id,
  customers.name,
  orders.total
FROM orders
INNER JOIN customers ON orders.customer_id = customers.id;`,
    expectedResults: {
      columns: ['order_id', 'name', 'total'],
      rows: [
        [1, 'Alice', 150.0],
        [2, 'Bob', 200.5],
      ],
    },
    database: 'northwind',
  }

  const callbacks: InteractiveExampleCallbacks = {
    onRunQuery: async (_query, _database) => {
      return {
        columns: ['order_id', 'name', 'total'],
        rows: [
          [1, 'Alice', 150.0],
          [2, 'Bob', 200.5],
        ],
      }
    },
  }

  return new InteractiveExample('example-container-3', config, callbacks)
}

// Integration with main app (example)
export function integrateWithWASM(): void {
  /*
  // Assuming you have WASM executor available
  import { executeQuery, loadDatabase } from '../db/wasm'

  const config: InteractiveExampleConfig = {
    title: 'Real WASM Example',
    initialQuery: 'SELECT 1 + 1 AS result;',
    database: 'empty',
  }

  const callbacks: InteractiveExampleCallbacks = {
    onRunQuery: async (query, database) => {
      // Execute query via WASM
      const result = await executeQuery(query)
      return {
        columns: result.columns,
        rows: result.rows,
      }
    },
    onLoadDatabase: async (database) => {
      await loadDatabase(database)
    },
  }

  const example = new InteractiveExample('container-id', config, callbacks)

  // Update theme when dark mode toggles
  document.addEventListener('theme-changed', () => {
    example.updateTheme()
  })

  // Cleanup when done
  // example.destroy()
  */
}

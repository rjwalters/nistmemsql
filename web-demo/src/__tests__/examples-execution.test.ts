import { describe, it, expect } from 'vitest'
import { getAllExamples } from '../data/examples'
import type { ExamplePayload } from '../data/examples/types'

describe('Example Result Validation Tests', () => {
  const allExamples = getAllExamples()

  describe('Examples with Expected Results', () => {
    const examplesWithExpectedResults = allExamples.filter(example => {
      const payload = example as ExamplePayload
      return payload.expectedRows || payload.expectedCount !== undefined
    })

    it('should have examples with expected results defined', () => {
      expect(examplesWithExpectedResults.length).toBeGreaterThan(0)
    })

    examplesWithExpectedResults.forEach(example => {
      it(`should have valid expected results structure for ${example.id}: ${example.title}`, () => {
        const payload = example as ExamplePayload

        // Check that the example has a database specified
        expect(example.database).toBeTruthy()
        expect(['northwind', 'employees']).toContain(example.database)

        // Check that SQL is defined
        expect(example.sql).toBeTruthy()
        expect(typeof example.sql).toBe('string')
        expect(example.sql.trim().length).toBeGreaterThan(0)

        // Validate expectedCount if present
        if (payload.expectedCount !== undefined) {
          expect(typeof payload.expectedCount).toBe('number')
          expect(payload.expectedCount).toBeGreaterThanOrEqual(0)
        }

        // Validate expectedRows if present
        if (payload.expectedRows) {
          expect(Array.isArray(payload.expectedRows)).toBe(true)
          expect(payload.expectedRows.length).toBeGreaterThan(0)

          // Each row should be an array
          payload.expectedRows.forEach((row, index) => {
            expect(Array.isArray(row), `Row ${index} should be an array`).toBe(true)
            expect(row.length).toBeGreaterThan(0)

            // Each cell should be a string (representing the expected value)
            row.forEach((cell, cellIndex) => {
              expect(typeof cell, `Cell [${index}][${cellIndex}] should be a string`).toBe('string')
            })
          })

          // If expectedCount is also specified, it should match the number of rows
          if (payload.expectedCount !== undefined) {
            expect(payload.expectedRows.length).toBe(payload.expectedCount)
          }
        }
      })
    })
  })

  describe('SQL Query Structure', () => {
    const selectExamples = allExamples.filter(example => {
      const payload = example as ExamplePayload
      return isSelectQuery(payload.sql)
    })

    const ddlExamples = allExamples.filter(example => {
      const payload = example as ExamplePayload
      return !isSelectQuery(payload.sql)
    })

    it('should have both SELECT and DDL/DML examples', () => {
      expect(selectExamples.length).toBeGreaterThan(0)
      expect(ddlExamples.length).toBeGreaterThan(0)
    })

    selectExamples.forEach(example => {
      it(`should have SELECT query for ${example.id}: ${example.title}`, () => {
        const payload = example as ExamplePayload
        expect(isSelectQuery(payload.sql)).toBe(true)
      })
    })

    ddlExamples.forEach(example => {
      it(`should have DDL/DML query for ${example.id}: ${example.title}`, () => {
        const payload = example as ExamplePayload
        expect(isSelectQuery(payload.sql)).toBe(false)
      })
    })
  })

  describe('Database Distribution', () => {
    const northwindExamples = allExamples.filter(ex => ex.database === 'northwind')
    const employeesExamples = allExamples.filter(ex => ex.database === 'employees')

    it('should have examples for both databases', () => {
      expect(northwindExamples.length).toBeGreaterThan(0)
      expect(employeesExamples.length).toBeGreaterThan(0)
    })

    it('should have northwind examples with expected results', () => {
      const withResults = northwindExamples.filter(ex => {
        const payload = ex as ExamplePayload
        return payload.expectedRows || payload.expectedCount !== undefined
      })
      expect(withResults.length).toBeGreaterThan(0)
    })

    it('should have employees examples with expected results', () => {
      const withResults = employeesExamples.filter(ex => {
        const payload = ex as ExamplePayload
        return payload.expectedRows || payload.expectedCount !== undefined
      })
      expect(withResults.length).toBeGreaterThan(0)
    })
  })
})

/**
 * Check if SQL query is a SELECT statement
 */
function isSelectQuery(sql: string): boolean {
  // Remove comments and normalize whitespace
  const cleanSql = sql
    .replace(/--.*$/gm, '') // Remove single-line comments
    .replace(/\/\*[\s\S]*?\*\//g, '') // Remove multi-line comments
    .trim()

  // Check if it starts with SELECT (case insensitive)
  return /^SELECT\s/i.test(cleanSql)
}

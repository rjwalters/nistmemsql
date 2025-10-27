import { describe, expect, it } from 'vitest'
import { validateSql } from '../editor/validation'

describe('validateSql', () => {
  it('accepts well-formed SQL', () => {
    const sql = "SELECT id, name FROM users WHERE active = TRUE AND name = 'Alice';"
    expect(validateSql(sql)).toHaveLength(0)
  })

  it('flags unmatched parentheses', () => {
    const sql = 'SELECT (1 + (2 * 3) FROM dual;'
    const issues = validateSql(sql)
    expect(issues.some((issue) => issue.message.includes('parenthesis'))).toBe(true)
  })

  it('flags unclosed single quotes', () => {
    const sql = "SELECT * FROM users WHERE name = 'Alice;"
    const issues = validateSql(sql)
    expect(issues.some((issue) => issue.message.includes('single quote'))).toBe(true)
  })

  it('flags unclosed quoted identifiers', () => {
    const sql = 'SELECT "column FROM data;'
    const issues = validateSql(sql)
    expect(issues.some((issue) => issue.message.includes('quoted identifier'))).toBe(true)
  })
})

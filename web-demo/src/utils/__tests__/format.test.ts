import { describe, it, expect } from 'vitest'
import { formatSqlValue, truncateString } from '../format'

describe('formatSqlValue', () => {
  it('should format null values', () => {
    expect(formatSqlValue(null)).toBe('NULL')
  })

  it('should format boolean values', () => {
    expect(formatSqlValue(true)).toBe('TRUE')
    expect(formatSqlValue(false)).toBe('FALSE')
  })

  it('should format number values', () => {
    expect(formatSqlValue(42)).toBe('42')
    expect(formatSqlValue(3.14)).toBe('3.14')
  })

  it('should format string values', () => {
    expect(formatSqlValue('hello')).toBe('hello')
    expect(formatSqlValue('')).toBe('')
  })
})

describe('truncateString', () => {
  it('should not truncate short strings', () => {
    expect(truncateString('hello', 10)).toBe('hello')
  })

  it('should truncate long strings with ellipsis', () => {
    expect(truncateString('hello world test', 10)).toBe('hello w...')
  })

  it('should handle exact length strings', () => {
    expect(truncateString('hello', 5)).toBe('hello')
  })

  it('should handle minimum truncation', () => {
    expect(truncateString('hello', 4)).toBe('h...')
  })
})

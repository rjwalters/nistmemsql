import { describe, it, expect } from 'vitest'

describe('Example Test Suite', () => {
  it('should perform basic arithmetic', () => {
    expect(2 + 2).toBe(4)
  })

  it('should handle string concatenation', () => {
    const greeting = 'Hello' + ' ' + 'World'
    expect(greeting).toBe('Hello World')
  })

  it('should work with arrays', () => {
    const arr = [1, 2, 3]
    expect(arr).toHaveLength(3)
    expect(arr).toContain(2)
  })
})

// Type guard example
function isString(value: unknown): value is string {
  return typeof value === 'string'
}

describe('Type Guards', () => {
  it('should correctly identify strings', () => {
    expect(isString('hello')).toBe(true)
    expect(isString(123)).toBe(false)
    expect(isString(null)).toBe(false)
  })
})

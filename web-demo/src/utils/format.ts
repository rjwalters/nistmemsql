/**
 * Format SQL value for display
 */
export function formatSqlValue(value: string | number | boolean | null): string {
  if (value === null) {
    return 'NULL'
  }
  if (typeof value === 'boolean') {
    return value ? 'TRUE' : 'FALSE'
  }
  if (typeof value === 'number') {
    return value.toString()
  }
  return value
}

/**
 * Truncate long strings for display
 */
export function truncateString(str: string, maxLength: number): string {
  if (str.length <= maxLength) {
    return str
  }
  return str.slice(0, maxLength - 3) + '...'
}

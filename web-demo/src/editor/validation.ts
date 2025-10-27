export interface SqlIssue {
  message: string
  offset: number
  length: number
}

/**
 * Perform heuristic validation to catch common SQL editing mistakes quickly.
 */
export function validateSql(sql: string): SqlIssue[] {
  const issues: SqlIssue[] = []
  const stack: { char: string; offset: number }[] = []

  let singleQuoteOffset = -1
  let doubleQuoteOffset = -1

  for (let i = 0; i < sql.length; i += 1) {
    const char = sql[i]

    switch (char) {
      case '(': {
        stack.push({ char, offset: i })
        break
      }
      case ')': {
        if (!stack.length) {
          issues.push({ message: 'Unmatched closing parenthesis', offset: i, length: 1 })
        } else {
          stack.pop()
        }
        break
      }
      case ''': {
        if (singleQuoteOffset >= 0 && sql[i - 1] !== '\') {
          singleQuoteOffset = -1
        } else if (singleQuoteOffset === -1) {
          singleQuoteOffset = i
        }
        break
      }
      case '"': {
        if (doubleQuoteOffset >= 0 && sql[i - 1] !== '\') {
          doubleQuoteOffset = -1
        } else if (doubleQuoteOffset === -1) {
          doubleQuoteOffset = i
        }
        break
      }
      default:
        break
    }
  }

  for (const unmatched of stack) {
    issues.push({
      message: 'Unmatched opening parenthesis',
      offset: unmatched.offset,
      length: 1,
    })
  }

  if (singleQuoteOffset >= 0) {
    issues.push({
      message: 'Unclosed single quote',
      offset: singleQuoteOffset,
      length: 1,
    })
  }

  if (doubleQuoteOffset >= 0) {
    issues.push({
      message: 'Unclosed quoted identifier',
      offset: doubleQuoteOffset,
      length: 1,
    })
  }

  return issues
}

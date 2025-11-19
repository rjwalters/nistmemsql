import type { Database, QueryResult, ExecuteResult } from '../db/types'
import type { MonacoEditor } from '../editor/monaco-loader'
import type { FallbackEditor } from '../editor/fallback-editor'

type Editor = MonacoEditor | FallbackEditor

function isQueryResult(result: QueryResult | ExecuteResult): result is QueryResult {
  return 'columns' in result && 'rows' in result
}

function formatValueForDisplay(value: unknown): string {
  if (value === null) {
    return 'NULL'
  }
  if (typeof value === 'string') {
    return value
  }
  return String(value)
}

function formatResultAsAsciiTable(result: QueryResult | ExecuteResult): string {
  if (!isQueryResult(result)) {
    // For execute results (INSERT, UPDATE, DELETE, etc.)
    const msg = result.message || 'Query executed successfully'
    const rows = result.rows_affected
    return rows !== undefined ? `${msg}\n(${rows} row${rows === 1 ? '' : 's'} affected)` : msg
  }

  if (result.rows.length === 0) {
    return '(0 rows)'
  }

  // Parse JSON strings into arrays
  const parsedRows = result.rows.map(rowStr => {
    try {
      return JSON.parse(rowStr) as unknown[]
    } catch {
      return [rowStr]
    }
  })

  // Calculate column widths
  const columnWidths: number[] = result.columns.map(col => col.length)
  parsedRows.forEach(row => {
    row.forEach((value, colIndex) => {
      const strValue = formatValueForDisplay(value)
      columnWidths[colIndex] = Math.max(columnWidths[colIndex], strValue.length)
    })
  })

  // Build the table
  const lines: string[] = []

  // Top border
  const topBorder = '+' + columnWidths.map(w => '-'.repeat(w + 2)).join('+') + '+'
  lines.push(topBorder)

  // Header row
  const headerCells = result.columns.map((col, i) => ` ${col.padEnd(columnWidths[i])} `)
  lines.push('|' + headerCells.join('|') + '|')

  // Header separator
  lines.push(topBorder)

  // Data rows
  parsedRows.forEach(row => {
    const cells = row.map((value, i) => {
      const strValue = formatValueForDisplay(value)
      return ` ${strValue.padEnd(columnWidths[i])} `
    })
    lines.push('|' + cells.join('|') + '|')
  })

  // Bottom border
  lines.push(topBorder)

  // Row count
  const rowCount = result.rows.length
  lines.push(`(${rowCount} row${rowCount === 1 ? '' : 's'})`)

  return lines.join('\n')
}

/**
 * Creates a query execution handler
 */
export function createExecutionHandler(
  editor: Editor,
  database: Database | null,
  resultsEditor: Editor,
  refreshTables: () => void
): () => void {
  return () => {
    const sql = editor.getValue().trim()

    if (!sql) {
      console.warn('Nothing to execute. Type a query first.')
      return
    }

    if (!database) {
      console.error('Database core is not ready yet.')
      const currentValue = resultsEditor.getValue()
      const errorMsg = '\n-- ERROR: Database not ready. Please refresh the page.\n'
      resultsEditor.setValue(currentValue + errorMsg)
      return
    }

    // Use setTimeout to allow UI to update before blocking execution
    setTimeout(() => {
      try {
        const startTime = performance.now()

        // Detect if this is a SELECT query
        // Skip SQL line comments (--) and whitespace before checking for SELECT
        const isSelectQuery = /^(?:\s*--[^\n]*\n)*\s*SELECT\s+/i.test(sql)

        const result = isSelectQuery ? database.query(sql) : database.execute(sql)

        const executionTime = performance.now() - startTime

        // Format the output
        const currentValue = resultsEditor.getValue()
        const separator = currentValue ? '\n\n' : ''
        const sqlComment = `-- Query:\n${sql}\n\n`
        const resultTable = formatResultAsAsciiTable(result)
        const timing = `\nExecuted in ${executionTime.toFixed(2)}ms`

        const newOutput = separator + sqlComment + resultTable + timing
        resultsEditor.setValue(currentValue + newOutput)

        // Scroll to bottom (Monaco-specific, skip for fallback editor)
        if ('getModel' in resultsEditor && 'revealLine' in resultsEditor) {
          const lineCount = resultsEditor.getModel()?.getLineCount() || 0
          resultsEditor.revealLine(lineCount)
        }

        refreshTables()
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error)
        const currentValue = resultsEditor.getValue()
        const separator = currentValue ? '\n\n' : ''
        const sqlComment = `-- Query:\n${sql}\n\n`
        const errorMsg = `ERROR: ${message}`
        resultsEditor.setValue(currentValue + separator + sqlComment + errorMsg)

        // Scroll to bottom (Monaco-specific, skip for fallback editor)
        if ('getModel' in resultsEditor && 'revealLine' in resultsEditor) {
          const lineCount = resultsEditor.getModel()?.getLineCount() || 0
          resultsEditor.revealLine(lineCount)
        }

        console.error(`Execution error: ${message}`)
      }
    }, 10)
  }
}

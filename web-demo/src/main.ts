import './styles/main.css'
import { initTheme } from './theme'
import { initDatabase } from './db/wasm'
import type { Database, QueryResult, ExecuteResult } from './db/types'
import { validateSql } from './editor/validation'
import { ThemeToggleComponent } from './components/ThemeToggle'
import { ExamplesComponent } from './components/Examples'
import type { ExampleSelectEvent } from './components/Examples'
import { DatabaseSelectorComponent } from './components/DatabaseSelector'
import type { DatabaseOption } from './components/DatabaseSelector'
import { initShowcase } from './showcase'
import { sampleDatabases, loadSampleDatabase, getSampleDatabase } from './data/sample-databases'

/* eslint-disable @typescript-eslint/no-explicit-any */
// Monaco types are loaded dynamically from CDN and not available at compile time
type Monaco = any
type MonacoEditor = any

declare global {
  interface Window {
    require?: any
    MonacoBasePath?: string
    monaco?: Monaco
  }
}
/* eslint-enable @typescript-eslint/no-explicit-any */

const SQL_KEYWORDS = [
  'SELECT',
  'FROM',
  'WHERE',
  'GROUP BY',
  'ORDER BY',
  'HAVING',
  'JOIN',
  'INNER JOIN',
  'LEFT JOIN',
  'RIGHT JOIN',
  'FULL JOIN',
  'ON',
  'LIMIT',
  'OFFSET',
  'INSERT',
  'UPDATE',
  'DELETE',
  'CREATE',
  'TABLE',
  'DROP',
  'ALTER',
  'VALUES',
  'SET',
  'DISTINCT',
  'COUNT',
  'SUM',
  'AVG',
  'MIN',
  'MAX',
]

const DEFAULT_SQL = `-- Welcome to NIST MemSQL Web Studio!
-- Press Ctrl/Cmd + Enter to run queries
-- Sample employees table is pre-loaded with 6 rows

-- See all employees
SELECT * FROM employees;

-- Try these queries too (uncomment and run):

-- Find engineering employees
-- SELECT name, salary FROM employees WHERE department = 'Engineering';

-- Count employees per department
-- SELECT department, COUNT(*) as count FROM employees GROUP BY department;

-- Find high earners
-- SELECT name, department, salary FROM employees WHERE salary > 80000;
`

async function loadMonaco(): Promise<Monaco> {
  const amdRequire = window.require

  if (!amdRequire) {
    throw new Error('Monaco AMD loader script not found. Did index.html include loader.min.js?')
  }

  const basePath =
    window.MonacoBasePath ?? 'https://cdnjs.cloudflare.com/ajax/libs/monaco-editor/0.44.0/min'

  return new Promise((resolve, reject) => {
    amdRequire.config({
      paths: {
        vs: basePath + '/vs',
      },
    })

    amdRequire(
      ['vs/editor/editor.main'],
      () => {
        if (window.monaco) {
          resolve(window.monaco)
        } else {
          reject(new Error('Monaco loaded but window.monaco is undefined'))
        }
      },
      (err: unknown) => reject(err instanceof Error ? err : new Error(String(err)))
    )
  })
}

function getLayoutElements(): {
  editorContainer: HTMLDivElement | null
  resultsEditorContainer: HTMLDivElement | null
  runButton: HTMLButtonElement | null
  themeToggleContainer: HTMLDivElement | null
} {
  return {
    editorContainer: document.querySelector<HTMLDivElement>('#editor'),
    resultsEditorContainer: document.querySelector<HTMLDivElement>('#results'),
    runButton: document.querySelector<HTMLButtonElement>('#execute-btn'),
    themeToggleContainer: document.querySelector<HTMLDivElement>('#theme-toggle'),
  }
}

function applyValidationMarkers(monaco: Monaco, editor: MonacoEditor): void {
  const model = editor.getModel()
  if (!model || !monaco.editor) return

  const sql = editor.getValue()
  const issues = validateSql(sql)
  const markers = issues.map(issue => {
    const start = model.getPositionAt(issue.offset)
    const end = model.getPositionAt(issue.offset + Math.max(issue.length, 1))

    return {
      severity: monaco.MarkerSeverity?.Error ?? 8,
      message: issue.message,
      startLineNumber: start.lineNumber,
      startColumn: start.column,
      endLineNumber: end.lineNumber,
      endColumn: end.column,
    }
  })

  monaco.editor.setModelMarkers(model, 'sql-validation', markers)
}

function registerCompletions(monaco: Monaco, getTableNames: () => string[]): void {
  if (!monaco.languages?.registerCompletionItemProvider) return

  monaco.languages.registerCompletionItemProvider('sql', {
    triggerCharacters: [' ', '.', '\n'],
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    provideCompletionItems(model: any, position: any) {
      const wordInfo = model.getWordUntilPosition(position)
      const range = {
        startLineNumber: position.lineNumber,
        endLineNumber: position.lineNumber,
        startColumn: wordInfo.startColumn,
        endColumn: wordInfo.endColumn,
      }

      const keywordSuggestions = SQL_KEYWORDS.filter(word =>
        word.toLowerCase().startsWith(wordInfo.word.toLowerCase())
      ).map(label => ({
        label,
        kind: monaco.languages.CompletionItemKind?.Keyword ?? 14,
        insertText: label,
        range,
      }))

      const tableSuggestions = getTableNames()
        .filter(table => table.toLowerCase().startsWith(wordInfo.word.toLowerCase()))
        .map(label => ({
          label,
          kind: monaco.languages.CompletionItemKind?.Field ?? 4,
          insertText: label,
          range,
        }))

      return {
        suggestions: [...keywordSuggestions, ...tableSuggestions],
      }
    },
  })
}

function registerShortcuts(monaco: Monaco, editor: MonacoEditor, execute: () => void): void {
  const { KeyMod = {}, KeyCode = {} } = monaco
  const ctrlEnter = (KeyMod.CtrlCmd ?? 0) | (KeyCode.Enter ?? 3)
  const ctrlSlash = (KeyMod.CtrlCmd ?? 0) | (KeyCode.US_SLASH ?? 85)

  editor.addCommand(ctrlEnter, execute)
  editor.addCommand(ctrlSlash, () => {
    const action = editor.getAction?.('editor.action.commentLine')
    if (action) {
      action.run()
    }
  })

  editor.addCommand(KeyCode.Tab ?? 2, () => {
    editor.trigger?.('keyboard', 'editor.action.indentLines', null)
  })
}

async function safeInitDatabase(): Promise<Database | null> {
  try {
    return await initDatabase()
  } catch (error) {
    console.warn('WASM database unavailable, SQL metadata features limited.', error)
    return null
  }
}

function setupThemeSync(themeToggleContainer: HTMLDivElement | null, monaco: Monaco): void {
  if (!themeToggleContainer) return

  const theme = initTheme()

  // Initialize ThemeToggleComponent - it will populate #theme-toggle automatically
  new ThemeToggleComponent(theme)

  const applyTheme = (mode: 'light' | 'dark'): void => {
    const targetTheme = mode === 'dark' ? 'vs-dark' : 'vs'
    monaco.editor?.setTheme?.(targetTheme)
  }

  applyTheme(theme.current)

  // Listen for theme changes
  const observer = new MutationObserver(() => {
    const isDark = document.documentElement.classList.contains('dark')
    applyTheme(isDark ? 'dark' : 'light')
  })
  observer.observe(document.documentElement, {
    attributes: true,
    attributeFilter: ['class'],
  })
}

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

function createExecutionHandler(
  editor: MonacoEditor,
  database: Database | null,
  resultsEditor: MonacoEditor,
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

        // Scroll to bottom
        const lineCount = resultsEditor.getModel()?.getLineCount() || 0
        resultsEditor.revealLine(lineCount)

        refreshTables()
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error)
        const currentValue = resultsEditor.getValue()
        const separator = currentValue ? '\n\n' : ''
        const sqlComment = `-- Query:\n${sql}\n\n`
        const errorMsg = `ERROR: ${message}`
        resultsEditor.setValue(currentValue + separator + sqlComment + errorMsg)

        // Scroll to bottom
        const lineCount = resultsEditor.getModel()?.getLineCount() || 0
        resultsEditor.revealLine(lineCount)

        console.error(`Execution error: ${message}`)
      }
    }, 10)
  }
}

async function bootstrap(): Promise<void> {
  const layout = getLayoutElements()

  if (!layout.editorContainer) {
    console.error('Failed to find #editor container')
    return
  }

  if (!layout.resultsEditorContainer) {
    console.error('Failed to find #results container')
    return
  }

  const monaco = await loadMonaco()
  const editor = monaco.editor.create(layout.editorContainer, {
    value: DEFAULT_SQL,
    language: 'sql',
    theme: 'vs-dark',
    minimap: { enabled: false },
    automaticLayout: true,
    fontFamily:
      'JetBrains Mono, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace',
    fontSize: 14,
    smoothScrolling: true,
    scrollBeyondLastLine: false,
  })

  const resultsEditor = monaco.editor.create(layout.resultsEditorContainer, {
    value: 'Query results will appear here\nPress Ctrl/Cmd + Enter to run queries',
    language: 'plaintext',
    theme: 'vs-dark',
    readOnly: true,
    minimap: { enabled: false },
    automaticLayout: true,
    fontFamily:
      'JetBrains Mono, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace',
    fontSize: 14,
    smoothScrolling: true,
    scrollBeyondLastLine: false,
    lineNumbers: 'off',
  })

  setupThemeSync(layout.themeToggleContainer, monaco)

  const database = await safeInitDatabase()
  let tableNames: string[] = []
  let currentDatabaseId = 'employees'

  // Function to load a database by ID
  const loadDatabase = (dbId: string): void => {
    if (!database) return

    const sampleDb = getSampleDatabase(dbId)
    if (!sampleDb) {
      console.error(`Unknown database: ${dbId}`)
      return
    }

    try {
      loadSampleDatabase(database, sampleDb)
      tableNames = database.list_tables()
      currentDatabaseId = dbId
    } catch (error) {
      console.warn(`Failed to load sample database ${dbId}:`, error)
    }
  }

  // Pre-load default sample database for immediate exploration
  if (database) {
    loadDatabase('employees')
  }

  registerCompletions(monaco, () => tableNames)

  const refreshTables = (): void => {
    if (!database) return
    try {
      tableNames = database.list_tables()
    } catch (error) {
      console.warn('Failed to refresh table metadata', error)
    }
  }

  refreshTables()

  editor.onDidChangeModelContent(() => {
    applyValidationMarkers(monaco, editor)
  })
  applyValidationMarkers(monaco, editor)

  // Initialize Examples sidebar
  const examplesComponent = new ExamplesComponent()
  examplesComponent.onSelect((event: ExampleSelectEvent) => {
    editor.setValue(event.sql)
    // Switch database if needed
    if (event.database !== currentDatabaseId) {
      loadDatabase(event.database)
      databaseSelector.setSelected(event.database)
    }
  })

  // Initialize Database Selector with all available sample databases
  const databases: DatabaseOption[] = sampleDatabases.map(db => ({
    id: db.id,
    name: db.name,
    description: db.description,
  }))
  const databaseSelector = new DatabaseSelectorComponent(databases, currentDatabaseId)
  databaseSelector.onChange((dbId: string) => {
    loadDatabase(dbId)
    refreshTables()
  })

  const execute = createExecutionHandler(editor, database, resultsEditor, refreshTables)

  registerShortcuts(monaco, editor, execute)
  layout.runButton?.addEventListener('click', execute)

  // Initialize SQL:1999 Showcase navigation
  initShowcase()

  // Set build timestamp in footer
  const timestampElement = document.getElementById('build-timestamp')
  if (timestampElement) {
    try {
      const timestamp = __BUILD_TIMESTAMP__
      const date = new Date(timestamp)
      const formattedDate = date.toLocaleString('en-US', {
        year: 'numeric',
        month: 'short',
        day: 'numeric',
        hour: '2-digit',
        minute: '2-digit',
        timeZoneName: 'short',
      })
      timestampElement.textContent = `Deployed: ${formattedDate}`
    } catch (error) {
      console.warn('Failed to set build timestamp', error)
      timestampElement.textContent = 'Deployed: Development build'
    }
  }
}

void bootstrap()

import './styles/main.css'
import { initTheme } from './theme'
import { initDatabase, getStorageMode, isOpfsSupported } from './db/wasm'
import type { Database, QueryResult, ExecuteResult } from './db/types'
import { validateSql } from './editor/validation'
import { NavigationComponent } from './components/Navigation'
import { ExamplesComponent } from './components/Examples'
import type { ExampleSelectEvent } from './components/Examples'
import { DatabaseSelectorComponent } from './components/DatabaseSelector'
import type { DatabaseOption } from './components/DatabaseSelector'
import { LoadingProgressComponent } from './components/LoadingProgress'
import { initShowcase } from './showcase'
import { sampleDatabases, loadSampleDatabase, getSampleDatabase, loadSqlDump } from './data/sample-databases'
import { updateConformanceFooter } from './utils/conformance'
import * as monaco from 'monaco-editor'

type Monaco = typeof monaco
type MonacoEditor = monaco.editor.IStandaloneCodeEditor

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
  // Monaco is now bundled with the app, so just return it
  return monaco
}

function getLayoutElements(): {
  editorContainer: HTMLDivElement | null
  resultsEditorContainer: HTMLDivElement | null
  runButton: HTMLButtonElement | null
} {
  return {
    editorContainer: document.querySelector<HTMLDivElement>('#editor'),
    resultsEditorContainer: document.querySelector<HTMLDivElement>('#results'),
    runButton: document.querySelector<HTMLButtonElement>('#execute-btn'),
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
  const KeyMod = monaco.KeyMod
  const KeyCode = monaco.KeyCode
  const ctrlEnter = KeyMod.CtrlCmd | KeyCode.Enter
  const ctrlSlash = KeyMod.CtrlCmd | KeyCode.Slash

  editor.addCommand(ctrlEnter, execute)
  editor.addCommand(ctrlSlash, () => {
    const action = editor.getAction('editor.action.commentLine')
    if (action) {
      action.run()
    }
  })

  editor.addCommand(KeyCode.Tab, () => {
    editor.trigger('keyboard', 'editor.action.indentLines', null)
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

function setupThemeSync(monaco: Monaco): { theme: ReturnType<typeof initTheme> } {
  const theme = initTheme()

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

  return { theme }
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
  // Initialize loading progress indicator
  const progress = new LoadingProgressComponent()
  progress.addStep('theme', 'Initializing theme')
  progress.addStep('monaco', 'Loading Monaco Editor')
  progress.addStep('wasm', 'Loading database engine')
  progress.addStep('ui', 'Setting up user interface')

  // Hide loading indicator when complete
  progress.onComplete(() => {
    setTimeout(() => progress.hide(), 500)
  })

  try {
    // Step 1: Initialize theme
    progress.updateStep('theme', 50, 'loading')
    const theme = initTheme()
    progress.completeStep('theme')

    const layout = getLayoutElements()

    if (!layout.editorContainer) {
      progress.errorStep('ui', 'Failed to find #editor container')
      console.error('Failed to find #editor container')
      return
    }

    if (!layout.resultsEditorContainer) {
      progress.errorStep('ui', 'Failed to find #results container')
      console.error('Failed to find #results container')
      return
    }

    // Step 2: Load Monaco Editor (this is the slowest part)
    progress.updateStep('monaco', 10, 'loading')
    const monacoStartTime = performance.now()

    // Simulate progress updates for Monaco loading
    const monacoProgressInterval = setInterval(() => {
      const elapsed = performance.now() - monacoStartTime
      // Estimate progress based on time (assume 1 second total)
      const estimatedProgress = Math.min(90, 10 + (elapsed / 1000) * 80)
      progress.updateStep('monaco', estimatedProgress, 'loading')
    }, 100)

    const monaco = await loadMonaco()
    clearInterval(monacoProgressInterval)
    progress.completeStep('monaco')

    // Step 3: Load WASM database in parallel with editor creation
    progress.updateStep('wasm', 20, 'loading')
    progress.updateStep('ui', 10, 'loading')

    const [database] = await Promise.all([
      safeInitDatabase().then(db => {
        progress.updateStep('wasm', 80, 'loading')
        return db
      }),
      // Create editors while WASM loads
      Promise.resolve().then(() => {
        progress.updateStep('ui', 30, 'loading')
        return null
      }),
    ])

    progress.completeStep('wasm')

    // Step 4: Create editor instances
    progress.updateStep('ui', 50, 'loading')
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

  setupThemeSync(monaco)

  // Initialize Navigation component
  progress.updateStep('ui', 70, 'loading')
  new NavigationComponent('terminal', theme)
  let tableNames: string[] = []
  let currentDatabaseId = 'employees'

  // Update storage status display
  const storageStatusEl = document.getElementById('storage-status')
  if (storageStatusEl) {
    const storageMode = getStorageMode()
    storageStatusEl.textContent = storageMode

    // Add visual indicator for OPFS vs in-memory
    if (isOpfsSupported() && storageMode.includes('OPFS')) {
      storageStatusEl.classList.add('text-green-600', 'dark:text-green-400')
      storageStatusEl.title = 'Data persists across browser sessions using Origin Private File System'
    } else {
      storageStatusEl.classList.add('text-yellow-600', 'dark:text-yellow-400')
      storageStatusEl.title = 'Data is temporary and will be lost when the page reloads'
    }
  }

  // Function to load a database by ID
  const loadDatabase = async (dbId: string): Promise<void> => {
    if (!database) return

    const sampleDb = getSampleDatabase(dbId)
    if (!sampleDb) {
      console.error(`Unknown database: ${dbId}`)
      return
    }

    try {
      // Load tables and create schema
      loadSampleDatabase(database, sampleDb)

      // For SQLLogicTest database, load data from SQL dump file
      if (dbId === 'sqllogictest') {
        await loadSqlDump(database, '/data/sqllogictest_results.sql')
      }

      tableNames = database.list_tables()
      currentDatabaseId = dbId
    } catch (error) {
      console.warn(`Failed to load sample database ${dbId}:`, error)
    }
  }

  // Pre-load default sample database for immediate exploration
  if (database) {
    void loadDatabase('employees')
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
      void loadDatabase(event.database)
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
    void loadDatabase(dbId).then(() => refreshTables())
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

  // Update conformance pass rate dynamically
  void updateConformanceFooter()

    // Final UI setup complete
    progress.updateStep('ui', 95, 'loading')

    // Small delay to show completion state
    await new Promise(resolve => setTimeout(resolve, 200))
    progress.completeStep('ui')
  } catch (error) {
    console.error('Bootstrap error:', error)
    const message = error instanceof Error ? error.message : String(error)
    progress.errorStep('ui', `Initialization failed: ${message}`)
  }
}

// Start the application when DOM is ready
document.addEventListener('DOMContentLoaded', () => void bootstrap())

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
import { loadMonaco, type Monaco, type MonacoEditor } from './editor/monaco-loader'
import { createFallbackEditor, type FallbackEditor } from './editor/fallback-editor'

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

// Editor state
type Editor = MonacoEditor | FallbackEditor

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

function setupThemeSync(monaco: Monaco | null): { theme: ReturnType<typeof initTheme> } {
  const theme = initTheme()

  const applyTheme = (mode: 'light' | 'dark'): void => {
    if (!monaco) return
    const targetTheme = mode === 'dark' ? 'vs-dark' : 'vs'
    monaco.editor?.setTheme?.(targetTheme)
  }

  if (monaco) {
    applyTheme(theme.current)
  }

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

/**
 * Upgrade from fallback editor to full Monaco editor
 */
async function upgradeToMonaco(
  container: HTMLElement,
  fallbackEditor: FallbackEditor,
  options: {
    value?: string
    language?: string
    theme?: string
    readOnly?: boolean
    lineNumbers?: 'on' | 'off'
  },
  callbacks: {
    onDidChangeModelContent?: (monaco: Monaco, editor: MonacoEditor) => void
    onShortcuts?: (monaco: Monaco, editor: MonacoEditor) => void
  }
): Promise<MonacoEditor> {
  console.log('[Editor Upgrade] Starting Monaco load...')

  // Get current value from fallback editor
  const currentValue = fallbackEditor.getValue()

  // Load Monaco
  const monaco = await loadMonaco()

  console.log('[Editor Upgrade] Creating Monaco editor...')

  // Destroy fallback editor
  fallbackEditor.destroy()

  // Clear container
  container.innerHTML = ''

  // Create Monaco editor
  const editor = monaco.editor.create(container, {
    value: currentValue || options.value || '',
    language: options.language || 'sql',
    theme: options.theme || 'vs-dark',
    readOnly: options.readOnly || false,
    minimap: { enabled: false },
    automaticLayout: true,
    fontFamily:
      'JetBrains Mono, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace',
    fontSize: 14,
    smoothScrolling: true,
    scrollBeyondLastLine: false,
    lineNumbers: options.lineNumbers || 'on',
  })

  // Apply current theme
  const isDark = document.documentElement.classList.contains('dark')
  monaco.editor.setTheme(isDark ? 'vs-dark' : 'vs')

  // Register callbacks
  if (callbacks.onDidChangeModelContent) {
    editor.onDidChangeModelContent(() => {
      callbacks.onDidChangeModelContent!(monaco, editor)
    })
  }

  if (callbacks.onShortcuts) {
    callbacks.onShortcuts(monaco, editor)
  }

  console.log('[Editor Upgrade] Monaco editor ready!')

  return editor
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

async function bootstrap(): Promise<void> {
  // Initialize loading progress indicator
  const progress = new LoadingProgressComponent()
  progress.addStep('theme', 'Initializing theme')
  progress.addStep('editor', 'Preparing editor')
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

    // Step 2: Create fallback editors (fast, no Monaco loading)
    progress.updateStep('editor', 50, 'loading')

    let editor: Editor = createFallbackEditor(layout.editorContainer, {
      value: DEFAULT_SQL,
      placeholder: 'Enter SQL query...',
    })

    let resultsEditor: Editor = createFallbackEditor(layout.resultsEditorContainer, {
      value: 'Query results will appear here\nPress Ctrl/Cmd + Enter to run queries',
      readOnly: true,
    })

    progress.completeStep('editor')

    // Step 3: Load WASM database in parallel with editor creation
    progress.updateStep('wasm', 20, 'loading')
    progress.updateStep('ui', 10, 'loading')

    const [database] = await Promise.all([
      safeInitDatabase().then(db => {
        progress.updateStep('wasm', 80, 'loading')
        return db
      }),
      // UI setup while WASM loads
      Promise.resolve().then(() => {
        progress.updateStep('ui', 30, 'loading')
        return null
      }),
    ])

    progress.completeStep('wasm')

    // Step 4: Initial UI setup (without Monaco)
    progress.updateStep('ui', 50, 'loading')

    setupThemeSync(null)

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

  const refreshTables = (): void => {
    if (!database) return
    try {
      tableNames = database.list_tables()
    } catch (error) {
      console.warn('Failed to refresh table metadata', error)
    }
  }

  refreshTables()

  // Track if Monaco has been loaded
  let monacoInstance: Monaco | null = null
  let hasUpgradedEditors = false
  let upgradePromise: Promise<void> | null = null

  // Mutable execute handler (will be recreated after Monaco upgrade)
  let executeHandler = createExecutionHandler(editor, database, resultsEditor, refreshTables)

  // Function to upgrade to Monaco on first interaction
  const upgradeEditorsToMonaco = async (): Promise<void> => {
    // Return existing promise if upgrade already in progress
    if (upgradePromise) return upgradePromise

    // Return immediately if already upgraded
    if (hasUpgradedEditors) return

    // Create and track the upgrade promise to prevent concurrent upgrades
    upgradePromise = (async () => {
      hasUpgradedEditors = true

      console.log('[Bootstrap] Upgrading to Monaco editors...')

      // Upgrade SQL editor
      const sqlEditor = await upgradeToMonaco(
        layout.editorContainer!,
        editor as FallbackEditor,
        {
          language: 'sql',
          theme: 'vs-dark',
        },
        {
          onDidChangeModelContent: (monaco, monacoEditor) => {
            applyValidationMarkers(monaco, monacoEditor)
          },
          onShortcuts: (monaco, monacoEditor) => {
            registerShortcuts(monaco, monacoEditor, () => executeHandler())
          },
        }
      )

      // Upgrade results editor
      const resultsMonacoEditor = await upgradeToMonaco(
        layout.resultsEditorContainer!,
        resultsEditor as FallbackEditor,
        {
          language: 'plaintext',
          theme: 'vs-dark',
          readOnly: true,
          lineNumbers: 'off',
        },
        {}
      )

      // Update references
      editor = sqlEditor
      resultsEditor = resultsMonacoEditor

      // Recreate execute handler with new Monaco editors
      executeHandler = createExecutionHandler(sqlEditor, database, resultsMonacoEditor, refreshTables)

      // Get Monaco instance
      monacoInstance = await loadMonaco()

      // Register completions with Monaco
      registerCompletions(monacoInstance, () => tableNames)

      // Apply initial validation
      applyValidationMarkers(monacoInstance, sqlEditor)

      // Update theme sync
      setupThemeSync(monacoInstance)
    })()

    return upgradePromise
  }

  // Add focus listener to upgrade editors
  layout.editorContainer?.addEventListener(
    'focus',
    () => {
      void upgradeEditorsToMonaco()
    },
    { capture: true, once: true }
  )

  // Also upgrade on click
  layout.editorContainer?.addEventListener(
    'click',
    () => {
      void upgradeEditorsToMonaco()
    },
    { once: true }
  )

  // Initialize Examples sidebar
  const examplesComponent = new ExamplesComponent()
  examplesComponent.onSelect((event: ExampleSelectEvent) => {
    // Upgrade to Monaco if needed before setting value
    void upgradeEditorsToMonaco().then(() => {
      editor.setValue(event.sql)
      // Switch database if needed
      if (event.database !== currentDatabaseId) {
        void loadDatabase(event.database)
        databaseSelector.setSelected(event.database)
      }
    })
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

  // Run button triggers upgrade and executes query
  layout.runButton?.addEventListener('click', () => {
    void upgradeEditorsToMonaco().then(() => executeHandler())
  })

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

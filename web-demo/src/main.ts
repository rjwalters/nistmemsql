import './styles/main.css'
import { initTheme } from './theme'
import { initDatabase } from './db/wasm'
import type { Database } from './db/types'
import { validateSql } from './editor/validation'
import { ResultsComponent } from './components/Results'
import { ThemeToggleComponent } from './components/ThemeToggle'
import { initShowcase } from './showcase'

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
  results: HTMLDivElement | null
  runButton: HTMLButtonElement | null
  themeToggleContainer: HTMLDivElement | null
} {
  return {
    editorContainer: document.querySelector<HTMLDivElement>('#editor'),
    results: document.querySelector<HTMLDivElement>('#results'),
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

function createExecutionHandler(
  editor: MonacoEditor,
  database: Database | null,
  resultsComponent: ResultsComponent,
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
      resultsComponent.showError('Database not ready. Please refresh the page.')
      return
    }

    resultsComponent.setLoading(true)

    // Use setTimeout to allow UI to update before blocking execution
    setTimeout(() => {
      try {
        const startTime = performance.now()

        // Detect if this is a SELECT query
        // Skip SQL line comments (--) and whitespace before checking for SELECT
        const isSelectQuery = /^(?:\s*--[^\n]*\n)*\s*SELECT\s+/i.test(sql)

        const result = isSelectQuery ? database.query(sql) : database.execute(sql)

        const executionTime = performance.now() - startTime

        resultsComponent.showResults(result, executionTime)
        refreshTables()
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error)
        resultsComponent.showError(message)
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

  setupThemeSync(layout.themeToggleContainer, monaco)

  const database = await safeInitDatabase()
  let tableNames: string[] = []
  if (database) {
    try {
      // Pre-load sample database for immediate exploration
      database.execute(
        'CREATE TABLE employees (id INTEGER, name VARCHAR(50), department VARCHAR(50), salary INTEGER);'
      )
      database.execute("INSERT INTO employees VALUES (1, 'Alice Johnson', 'Engineering', 95000);")
      database.execute("INSERT INTO employees VALUES (2, 'Bob Smith', 'Engineering', 87000);")
      database.execute("INSERT INTO employees VALUES (3, 'Carol White', 'Sales', 72000);")
      database.execute("INSERT INTO employees VALUES (4, 'David Brown', 'Sales', 68000);")
      database.execute("INSERT INTO employees VALUES (5, 'Eve Martinez', 'HR', 65000);")
      database.execute("INSERT INTO employees VALUES (6, 'Frank Wilson', 'Engineering', 92000);")
      tableNames = database.list_tables()
    } catch (error) {
      console.warn('Failed to load sample data', error)
    }
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

  const resultsComponent = new ResultsComponent()

  const execute = createExecutionHandler(editor, database, resultsComponent, refreshTables)

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

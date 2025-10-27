import './styles/main.css'
import { initTheme } from './theme'
import { initDatabase } from './db/wasm'
import type { Database } from './db/types'
import { validateSql } from './editor/validation'
import { ResultsComponent } from './components/Results'
import { HelpModal } from './components/HelpModal'
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
-- Press Ctrl/Cmd + Enter to execute queries

-- Create a sample table
CREATE TABLE products (
  id INTEGER,
  name VARCHAR(100),
  price INTEGER,
  category VARCHAR(50)
);

-- Insert sample data
INSERT INTO products VALUES (1, 'Laptop', 999, 'Electronics');
INSERT INTO products VALUES (2, 'Mouse', 25, 'Electronics');
INSERT INTO products VALUES (3, 'Desk Chair', 299, 'Furniture');
INSERT INTO products VALUES (4, 'Monitor', 449, 'Electronics');

-- Query the data
SELECT * FROM products;
`

type StatusVariant = 'info' | 'success' | 'error'

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

function createLayout(root: HTMLElement): {
  editorContainer: HTMLDivElement | null
  statusBar: HTMLDivElement | null
  results: HTMLDivElement | null
  resultMeta: HTMLSpanElement | null
  runButton: HTMLButtonElement | null
  themeToggle: HTMLButtonElement | null
  helpButton: HTMLButtonElement | null
} {
  root.innerHTML = `
    <div class="app-shell">
      <header class="app-header">
        <div>
          <h1 class="app-header__title">NIST MemSQL Web Studio</h1>
          <p class="shortcut-badge">
            <span class="shortcut-key">⌘ / Ctrl + Enter</span>
            <span>Run query</span>
          </p>
        </div>
        <div class="app-header__actions">
          <button id="help-button" class="button secondary" type="button" title="Keyboard shortcuts (? or F1)">
            Help
          </button>
          <button id="toggle-theme" class="button secondary" type="button">
            Toggle theme
          </button>
          <button id="run-query" class="button" type="button">
            Run query
          </button>
        </div>
      </header>
      <div class="app-body">
        <section class="panel">
          <div id="editor" class="editor-container"></div>
          <div id="status-bar" class="status-bar status-bar--info">
            Initializing Monaco editor…
          </div>
        </section>
        <section class="panel panel--results">
          <div class="results-heading">
            <span>Results</span>
            <span id="result-meta" class="shortcut-badge">No query executed yet</span>
          </div>
          <div id="results" class="results-empty">
            Run a query to see results here.
          </div>
        </section>
      </div>
      <div id="help-modal"></div>
    </div>
  `

  return {
    editorContainer: root.querySelector<HTMLDivElement>('#editor'),
    statusBar: root.querySelector<HTMLDivElement>('#status-bar'),
    results: root.querySelector<HTMLDivElement>('#results'),
    resultMeta: root.querySelector<HTMLSpanElement>('#result-meta'),
    runButton: root.querySelector<HTMLButtonElement>('#run-query'),
    themeToggle: root.querySelector<HTMLButtonElement>('#toggle-theme'),
    helpButton: root.querySelector<HTMLButtonElement>('#help-button'),
  } as const
}

function updateStatus(
  element: HTMLDivElement | null,
  message: string,
  variant: StatusVariant
): void {
  if (!element) return

  element.textContent = message
  element.classList.remove('status-bar--info', 'status-bar--success', 'status-bar--error')
  element.classList.add(`status-bar--${variant}`)
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

function setupThemeSync(themeToggle: HTMLButtonElement | null, monaco: Monaco): void {
  const theme = initTheme()

  const applyTheme = (mode: 'light' | 'dark'): void => {
    const targetTheme = mode === 'dark' ? 'vs-dark' : 'vs'
    monaco.editor?.setTheme?.(targetTheme)
  }

  applyTheme(theme.current)

  themeToggle?.addEventListener('click', () => {
    const isDark = theme.toggle()
    applyTheme(isDark ? 'dark' : 'light')
  })
}

function createExecutionHandler(
  editor: MonacoEditor,
  database: Database | null,
  statusBar: HTMLDivElement | null,
  resultsComponent: ResultsComponent,
  refreshTables: () => void
): () => void {
  return () => {
    const sql = editor.getValue().trim()

    if (!sql) {
      updateStatus(statusBar, 'Nothing to execute. Type a query first.', 'info')
      return
    }

    if (!database) {
      updateStatus(
        statusBar,
        'Database core is not ready yet. Build the WASM module to enable execution.',
        'error'
      )
      return
    }

    resultsComponent.setLoading(true)
    updateStatus(statusBar, 'Executing query...', 'info')

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
        updateStatus(statusBar, 'Query executed successfully.', 'success')
        refreshTables()
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error)
        resultsComponent.showError(message)
        updateStatus(statusBar, `Execution error: ${message}`, 'error')
      }
    }, 10)
  }
}

async function bootstrap(): Promise<void> {
  const appRoot = document.getElementById('app')
  if (!appRoot) {
    console.error('Failed to find #app container')
    return
  }

  const layout = createLayout(appRoot)
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

  setupThemeSync(layout.themeToggle, monaco)

  const database = await safeInitDatabase()
  let tableNames: string[] = []
  if (database) {
    try {
      tableNames = database.list_tables()
    } catch (error) {
      console.warn('Failed to fetch table metadata', error)
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
  const helpModal = new HelpModal()

  const execute = createExecutionHandler(
    editor,
    database,
    layout.statusBar,
    resultsComponent,
    refreshTables
  )

  registerShortcuts(monaco, editor, execute)
  layout.runButton?.addEventListener('click', execute)
  layout.helpButton?.addEventListener('click', () => helpModal.open())

  // Initialize SQL:1999 Showcase navigation
  initShowcase()

  if (layout.statusBar) {
    layout.statusBar.textContent = 'Monaco editor ready. Write SQL and run with Ctrl/Cmd + Enter.'
    layout.statusBar.classList.remove('status-bar--info')
    layout.statusBar.classList.add('status-bar--success')
  }
}

void bootstrap()

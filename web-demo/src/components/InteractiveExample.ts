export interface InteractiveExampleConfig {
  title: string
  description?: string
  initialQuery: string
  expectedResults?: {
    columns: string[]
    rows: unknown[][]
  }
  database?: 'northwind' | 'employees' | 'empty'
}

export interface InteractiveExampleCallbacks {
  onRunQuery: (query: string, database: string) => Promise<{ columns: string[]; rows: unknown[][] }>
  onLoadDatabase?: (database: string) => Promise<void>
}

/**
 * Interactive Example Framework Component
 *
 * Provides a self-contained query execution environment with:
 * - Pre-populated Monaco editor with example query
 * - "Run Example" button for one-click execution
 * - Expected vs actual results comparison
 * - Editable query area for experimentation
 * - Reset button to restore original query
 * - Error handling and display
 */
export class InteractiveExample {
  private container: HTMLElement
  private config: InteractiveExampleConfig
  private callbacks: InteractiveExampleCallbacks
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private editor: any = null
  private currentQuery: string
  private isExecuting = false

  constructor(
    containerId: string,
    config: InteractiveExampleConfig,
    callbacks: InteractiveExampleCallbacks
  ) {
    const container = document.getElementById(containerId)
    if (!container) {
      throw new Error(`Container '${containerId}' not found`)
    }

    this.container = container
    this.config = config
    this.callbacks = callbacks
    this.currentQuery = config.initialQuery

    this.render()
    this.setupEditor()
    this.setupEventListeners()
  }

  private render(): void {
    this.container.innerHTML = `
      <div class="interactive-example bg-white dark:bg-gray-800 rounded-lg shadow-lg p-6">
        <!-- Header -->
        <div class="mb-4">
          <h3 class="text-lg font-semibold text-gray-900 dark:text-gray-100">
            ${this.escapeHtml(this.config.title)}
          </h3>
          ${this.config.description ? `
            <p class="text-sm text-gray-600 dark:text-gray-400 mt-1">
              ${this.escapeHtml(this.config.description)}
            </p>
          ` : ''}
        </div>

        <!-- Query Editor -->
        <div class="mb-4">
          <div class="flex items-center justify-between mb-2">
            <label class="text-sm font-medium text-gray-700 dark:text-gray-300">
              Query
            </label>
            <div class="flex gap-2">
              <button
                id="reset-query-btn"
                class="px-3 py-1 text-sm bg-gray-100 hover:bg-gray-200 dark:bg-gray-700 dark:hover:bg-gray-600 text-gray-700 dark:text-gray-300 rounded transition-colors"
                title="Reset to original query"
              >
                Reset
              </button>
              <button
                id="run-query-btn"
                class="px-4 py-1 text-sm bg-blue-600 hover:bg-blue-700 text-white font-medium rounded transition-colors"
              >
                Run Example
              </button>
            </div>
          </div>
          <div id="example-editor" class="border border-gray-300 dark:border-gray-600 rounded" style="height: 150px;"></div>
        </div>

        <!-- Results Panel -->
        <div class="mb-4">
          <label class="text-sm font-medium text-gray-700 dark:text-gray-300 mb-2 block">
            Results
          </label>
          <div id="results-panel" class="border border-gray-300 dark:border-gray-600 rounded p-4 bg-gray-50 dark:bg-gray-900">
            <p class="text-sm text-gray-500 dark:text-gray-400 italic">
              Click "Run Example" to execute the query
            </p>
          </div>
        </div>

        <!-- Expected Results (if provided) -->
        ${this.config.expectedResults ? `
          <div class="mb-4">
            <label class="text-sm font-medium text-gray-700 dark:text-gray-300 mb-2 block">
              Expected Results
            </label>
            <div class="border border-gray-300 dark:border-gray-600 rounded p-4 bg-gray-50 dark:bg-gray-900">
              ${this.renderExpectedResults()}
            </div>
          </div>
        ` : ''}

        <!-- Status Message -->
        <div id="status-message" class="hidden"></div>
      </div>
    `
  }

  private setupEditor(): void {
    const editorContainer = document.getElementById('example-editor')
    if (!editorContainer) return

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const monaco = (window as any).monaco
    if (!monaco) {
      console.error('Monaco editor not loaded')
      return
    }

    this.editor = monaco.editor.create(editorContainer, {
      value: this.currentQuery,
      language: 'sql',
      theme: this.getTheme(),
      minimap: { enabled: false },
      lineNumbers: 'on',
      scrollBeyondLastLine: false,
      automaticLayout: true,
      fontSize: 14,
      tabSize: 2,
    })

    // Update currentQuery when editor content changes
    this.editor.onDidChangeModelContent(() => {
      this.currentQuery = this.editor?.getValue() || ''
    })
  }

  private getTheme(): string {
    return document.documentElement.classList.contains('dark') ? 'vs-dark' : 'vs'
  }

  private setupEventListeners(): void {
    // Run query button
    const runBtn = document.getElementById('run-query-btn')
    runBtn?.addEventListener('click', () => this.runQuery())

    // Reset button
    const resetBtn = document.getElementById('reset-query-btn')
    resetBtn?.addEventListener('click', () => this.resetQuery())
  }

  private async runQuery(): Promise<void> {
    if (this.isExecuting) return

    this.isExecuting = true
    this.showStatus('Executing query...', 'info')

    try {
      // Load database if needed
      if (this.config.database && this.callbacks.onLoadDatabase) {
        await this.callbacks.onLoadDatabase(this.config.database)
      }

      // Execute query
      const results = await this.callbacks.onRunQuery(
        this.currentQuery,
        this.config.database || 'empty'
      )

      // Display results
      this.displayResults(results)
      this.showStatus('Query executed successfully', 'success')
    } catch (error) {
      this.displayError(error instanceof Error ? error.message : String(error))
      this.showStatus('Query execution failed', 'error')
    } finally {
      this.isExecuting = false
    }
  }

  private resetQuery(): void {
    this.currentQuery = this.config.initialQuery
    if (this.editor) {
      this.editor.setValue(this.currentQuery)
    }
    this.showStatus('Query reset to original', 'info')
  }

  private displayResults(results: { columns: string[]; rows: unknown[][] }): void {
    const resultsPanel = document.getElementById('results-panel')
    if (!resultsPanel) return

    if (results.rows.length === 0) {
      resultsPanel.innerHTML = `
        <p class="text-sm text-gray-500 dark:text-gray-400 italic">
          Query executed successfully (0 rows)
        </p>
      `
      return
    }

    resultsPanel.innerHTML = `
      <div class="overflow-x-auto">
        <table class="w-full text-sm">
          <thead class="bg-gray-100 dark:bg-gray-800">
            <tr>
              ${results.columns.map(col => `
                <th class="px-3 py-2 text-left font-medium text-gray-700 dark:text-gray-300">
                  ${this.escapeHtml(col)}
                </th>
              `).join('')}
            </tr>
          </thead>
          <tbody class="divide-y divide-gray-200 dark:divide-gray-700">
            ${results.rows.map(row => `
              <tr class="hover:bg-gray-50 dark:hover:bg-gray-800/50">
                ${row.map(cell => `
                  <td class="px-3 py-2 text-gray-900 dark:text-gray-100">
                    ${this.formatCell(cell)}
                  </td>
                `).join('')}
              </tr>
            `).join('')}
          </tbody>
        </table>
      </div>
      <p class="text-xs text-gray-500 dark:text-gray-400 mt-2">
        ${results.rows.length} row${results.rows.length === 1 ? '' : 's'} returned
      </p>
    `
  }

  private displayError(error: string): void {
    const resultsPanel = document.getElementById('results-panel')
    if (!resultsPanel) return

    resultsPanel.innerHTML = `
      <div class="bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded p-3">
        <p class="text-sm font-medium text-red-800 dark:text-red-200 mb-1">
          Error executing query
        </p>
        <p class="text-xs text-red-700 dark:text-red-300 font-mono">
          ${this.escapeHtml(error)}
        </p>
      </div>
    `
  }

  private renderExpectedResults(): string {
    if (!this.config.expectedResults) return ''

    const { columns, rows } = this.config.expectedResults

    if (rows.length === 0) {
      return `<p class="text-sm text-gray-500 dark:text-gray-400 italic">Empty result set</p>`
    }

    return `
      <div class="overflow-x-auto">
        <table class="w-full text-sm">
          <thead class="bg-gray-100 dark:bg-gray-800">
            <tr>
              ${columns.map(col => `
                <th class="px-3 py-2 text-left font-medium text-gray-700 dark:text-gray-300">
                  ${this.escapeHtml(col)}
                </th>
              `).join('')}
            </tr>
          </thead>
          <tbody class="divide-y divide-gray-200 dark:divide-gray-700">
            ${rows.map(row => `
              <tr>
                ${row.map(cell => `
                  <td class="px-3 py-2 text-gray-900 dark:text-gray-100">
                    ${this.formatCell(cell)}
                  </td>
                `).join('')}
              </tr>
            `).join('')}
          </tbody>
        </table>
      </div>
    `
  }

  private formatCell(value: unknown): string {
    if (value === null || value === undefined) {
      return '<span class="text-gray-400 italic">NULL</span>'
    }
    return this.escapeHtml(String(value))
  }

  private showStatus(message: string, type: 'info' | 'success' | 'error'): void {
    const statusEl = document.getElementById('status-message')
    if (!statusEl) return

    const colorClasses = {
      info: 'bg-blue-50 dark:bg-blue-900/20 border-blue-200 dark:border-blue-800 text-blue-800 dark:text-blue-200',
      success: 'bg-green-50 dark:bg-green-900/20 border-green-200 dark:border-green-800 text-green-800 dark:text-green-200',
      error: 'bg-red-50 dark:bg-red-900/20 border-red-200 dark:border-red-800 text-red-800 dark:text-red-200',
    }

    statusEl.className = `border rounded p-3 text-sm ${colorClasses[type]}`
    statusEl.textContent = message
    statusEl.classList.remove('hidden')

    // Auto-hide after 3 seconds
    setTimeout(() => {
      statusEl.classList.add('hidden')
    }, 3000)
  }

  private escapeHtml(text: string): string {
    const map: Record<string, string> = {
      '&': '&amp;',
      '<': '&lt;',
      '>': '&gt;',
      '"': '&quot;',
      "'": '&#039;',
    }
    return text.replace(/[&<>"']/g, m => map[m])
  }

  /**
   * Update the theme (call this when dark mode toggles)
   */
  public updateTheme(): void {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const monaco = (window as any).monaco
    if (this.editor && monaco) {
      monaco.editor.setTheme(this.getTheme())
    }
  }

  /**
   * Cleanup resources
   */
  public destroy(): void {
    if (this.editor) {
      this.editor.dispose()
      this.editor = null
    }
  }
}

import { Component } from './base'
import type { QueryResult } from '../db/types'

interface ResultsState {
  result: QueryResult | null
  error: string | null
  loading: boolean
}

/**
 * Results component displays query results in a table format
 */
export class ResultsComponent extends Component<ResultsState> {
  constructor() {
    super('#results', {
      result: null,
      error: null,
      loading: false,
    })
  }

  /**
   * Show query results
   */
  showResults(result: QueryResult): void {
    this.setState({ result, error: null, loading: false })
  }

  /**
   * Show error message
   */
  showError(error: string): void {
    this.setState({ error, result: null, loading: false })
  }

  /**
   * Set loading state
   */
  setLoading(loading: boolean): void {
    this.setState({ loading })
  }

  protected render(): void {
    if (this.state.loading) {
      this.element.innerHTML = this.renderLoading()
      return
    }

    if (this.state.error) {
      this.element.innerHTML = this.renderError(this.state.error)
      return
    }

    if (this.state.result) {
      this.element.innerHTML = this.renderTable(this.state.result)
    } else {
      this.element.innerHTML = this.renderEmpty()
    }
  }

  private renderLoading(): string {
    return `
      <div class="flex items-center justify-center p-8">
        <div class="animate-spin h-8 w-8 border-4 border-primary-light border-t-transparent rounded-full"></div>
      </div>
    `
  }

  private renderError(error: string): string {
    return `
      <div class="bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-lg p-4">
        <p class="text-red-700 dark:text-red-300 font-mono text-sm">${this.escapeHtml(error)}</p>
      </div>
    `
  }

  private renderEmpty(): string {
    return `
      <div class="text-gray-500 dark:text-gray-400 text-center p-8">
        <p>Execute a query to see results</p>
      </div>
    `
  }

  private renderTable(result: QueryResult): string {
    const { columns, rows, row_count } = result

    if (rows.length === 0) {
      return `
        <div class="text-gray-600 dark:text-gray-400 p-4">
          <p>Query executed successfully (0 rows)</p>
        </div>
      `
    }

    return `
      <div class="overflow-x-auto">
        <table class="results-table">
          <thead>
            <tr>
              ${columns.map(col => `<th>${this.escapeHtml(col)}</th>`).join('')}
            </tr>
          </thead>
          <tbody>
            ${rows.map(row => `
              <tr>
                ${row.map(cell => `<td>${this.formatCell(cell)}</td>`).join('')}
              </tr>
            `).join('')}
          </tbody>
        </table>
        <div class="text-sm text-gray-600 dark:text-gray-400 mt-2 px-4">
          ${row_count} row${row_count === 1 ? '' : 's'}
        </div>
      </div>
    `
  }

  private formatCell(value: unknown): string {
    if (value === null) {
      return '<span class="text-gray-400 italic">NULL</span>'
    }
    return this.escapeHtml(String(value))
  }
}

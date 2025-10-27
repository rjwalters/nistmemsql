import { Component } from './base'

interface EditorState {
  value: string
}

/**
 * SQL Editor component - simple textarea wrapper
 * (Monaco Editor integration can be added later)
 */
export class EditorComponent extends Component<EditorState> {
  private textarea: HTMLTextAreaElement | null = null
  private onExecuteCallback: ((sql: string) => void) | null = null

  constructor() {
    super('#editor', {
      value: '',
    })
  }

  /**
   * Register callback for query execution
   */
  onExecute(callback: (sql: string) => void): void {
    this.onExecuteCallback = callback
  }

  /**
   * Get current SQL value
   */
  getValue(): string {
    return this.state.value
  }

  /**
   * Set SQL value
   */
  setValue(value: string): void {
    this.setState({ value })
    if (this.textarea) {
      this.textarea.value = value
    }
  }

  protected render(): void {
    this.element.innerHTML = `
      <div class="sql-editor">
        <textarea
          id="sql-input"
          class="w-full h-64 p-4 font-mono text-sm bg-white dark:bg-gray-900 text-gray-900 dark:text-gray-100 resize-y focus:outline-none focus:ring-2 focus:ring-primary-light"
          placeholder="Enter SQL query here... (Ctrl+Enter or Cmd+Enter to execute)"
          aria-label="SQL Query Editor"
        >${this.escapeHtml(this.state.value)}</textarea>
      </div>
    `

    // Get textarea reference and setup event listeners
    this.textarea = this.element.querySelector('#sql-input')
    if (this.textarea) {
      // Update state when textarea changes
      this.textarea.addEventListener('input', (e) => {
        this.state.value = (e.target as HTMLTextAreaElement).value
      })

      // Handle keyboard shortcuts
      this.textarea.addEventListener('keydown', (e) => {
        // Ctrl+Enter or Cmd+Enter to execute
        if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
          e.preventDefault()
          if (this.onExecuteCallback) {
            this.onExecuteCallback(this.state.value)
          }
        }
      })
    }
  }
}

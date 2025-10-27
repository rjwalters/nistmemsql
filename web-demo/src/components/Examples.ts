import { Component } from './base'

interface Example {
  title: string
  sql: string
  description: string
}

interface ExamplesState {
  examples: Example[]
  expanded: boolean
}

/**
 * Examples component shows a library of example SQL queries
 */
export class ExamplesComponent extends Component<ExamplesState> {
  private onSelectCallback: ((sql: string) => void) | null = null

  constructor() {
    super('#examples', {
      examples: [
        {
          title: 'Create Table',
          description: 'Create a simple users table',
          sql: 'CREATE TABLE users (\n  id INTEGER PRIMARY KEY,\n  name VARCHAR(100),\n  age INTEGER\n);',
        },
        {
          title: 'Insert Data',
          description: 'Insert sample data into users table',
          sql: "INSERT INTO users (id, name, age) VALUES\n  (1, 'Alice', 30),\n  (2, 'Bob', 25),\n  (3, 'Charlie', 35);",
        },
        {
          title: 'Select All',
          description: 'Query all users',
          sql: 'SELECT * FROM users;',
        },
        {
          title: 'Filter Query',
          description: 'Find users over 25',
          sql: 'SELECT * FROM users WHERE age > 25 ORDER BY name;',
        },
        {
          title: 'Aggregation',
          description: 'Calculate average age',
          sql: 'SELECT COUNT(*) as total, AVG(age) as average_age FROM users;',
        },
      ],
      expanded: true,
    })
  }

  /**
   * Register callback when example is selected
   */
  onSelect(callback: (sql: string) => void): void {
    this.onSelectCallback = callback
  }

  /**
   * Toggle expanded state
   */
  toggle(): void {
    this.setState({ expanded: !this.state.expanded })
  }

  protected render(): void {
    const { examples, expanded } = this.state

    this.element.innerHTML = `
      <div class="bg-white dark:bg-gray-800 rounded-lg shadow-lg">
        <button
          id="examples-toggle"
          class="w-full px-4 py-3 text-left font-semibold text-gray-900 dark:text-gray-100 hover:bg-gray-50 dark:hover:bg-gray-700 transition-colors flex justify-between items-center"
          aria-expanded="${expanded}"
          aria-controls="examples-list"
        >
          <span>Example Queries</span>
          <span class="text-gray-500">${expanded ? '▼' : '▶'}</span>
        </button>

        ${expanded ? `
          <div id="examples-list" class="border-t border-gray-200 dark:border-gray-700">
            ${examples.map((ex, idx) => `
              <button
                class="example-item w-full px-4 py-3 text-left border-b border-gray-100 dark:border-gray-700 last:border-b-0 hover:bg-gray-50 dark:hover:bg-gray-700 transition-colors"
                data-index="${idx}"
                aria-label="Load ${this.escapeHtml(ex.title)} example"
              >
                <div class="font-medium text-sm text-gray-900 dark:text-gray-100">
                  ${this.escapeHtml(ex.title)}
                </div>
                <div class="text-xs text-gray-600 dark:text-gray-400 mt-1">
                  ${this.escapeHtml(ex.description)}
                </div>
              </button>
            `).join('')}
          </div>
        ` : ''}
      </div>
    `

    // Setup event listeners
    const toggleBtn = this.element.querySelector('#examples-toggle')
    if (toggleBtn) {
      toggleBtn.addEventListener('click', () => this.toggle())
    }

    const exampleItems = this.element.querySelectorAll('.example-item')
    exampleItems.forEach(item => {
      item.addEventListener('click', () => {
        const index = parseInt(item.getAttribute('data-index') || '0')
        const example = this.state.examples[index]
        if (example && this.onSelectCallback) {
          this.onSelectCallback(example.sql)
        }
      })
    })
  }
}

import { Component } from './base'
import { exampleCategories, type QueryExample } from '../data/examples'

interface ExamplesState {
  expandedCategories: Set<string>
}

export interface ExampleSelectEvent {
  sql: string
  database: 'northwind' | 'employees' | 'empty'
}

/**
 * Examples component shows a library of example SQL queries
 * organized by SQL:1999 feature categories
 */
export class ExamplesComponent extends Component<ExamplesState> {
  private onSelectCallback: ((event: ExampleSelectEvent) => void) | null = null

  constructor() {
    super('#examples', {
      expandedCategories: new Set(['basic']), // Expand 'basic' category by default
    })
  }

  /**
   * Register callback when example is selected
   */
  onSelect(callback: (event: ExampleSelectEvent) => void): void {
    this.onSelectCallback = callback
  }

  /**
   * Toggle category expanded state
   */
  toggleCategory(categoryId: string): void {
    const expanded = new Set(this.state.expandedCategories)
    if (expanded.has(categoryId)) {
      expanded.delete(categoryId)
    } else {
      expanded.add(categoryId)
    }
    this.setState({ expandedCategories: expanded })
  }

  /**
   * Handle example selection
   */
  private selectExample(example: QueryExample): void {
    if (this.onSelectCallback) {
      this.onSelectCallback({
        sql: example.sql,
        database: example.database,
      })
    }
  }

  protected render(): void {
    const { expandedCategories } = this.state

    this.element.innerHTML = `
      <div class="bg-white dark:bg-gray-800 rounded-lg shadow-lg">
        <div class="px-4 py-3 border-b border-gray-200 dark:border-gray-700">
          <h2 class="font-semibold text-gray-900 dark:text-gray-100">
            SQL:1999 Examples
          </h2>
          <p class="text-xs text-gray-600 dark:text-gray-400 mt-1">
            ${this.getTotalExampleCount()} queries across ${exampleCategories.length} categories
          </p>
        </div>

        <div class="divide-y divide-gray-200 dark:divide-gray-700">
          ${exampleCategories.map(category => this.renderCategory(category, expandedCategories)).join('')}
        </div>
      </div>
    `

    // Setup event listeners
    this.setupEventListeners()
  }

  private getTotalExampleCount(): number {
    return exampleCategories.reduce((sum, cat) => sum + cat.queries.length, 0)
  }

  private renderCategory(category: typeof exampleCategories[0], expandedCategories: Set<string>): string {
    const isExpanded = expandedCategories.has(category.id)

    return `
      <div class="category-section">
        <button
          class="category-toggle w-full px-4 py-3 text-left hover:bg-gray-50 dark:hover:bg-gray-700 transition-colors flex justify-between items-center"
          data-category-id="${this.escapeHtml(category.id)}"
          aria-expanded="${isExpanded}"
          aria-controls="category-${this.escapeHtml(category.id)}"
        >
          <div class="flex-1">
            <div class="font-medium text-sm text-gray-900 dark:text-gray-100">
              ${this.escapeHtml(category.title)}
            </div>
            <div class="text-xs text-gray-600 dark:text-gray-400 mt-1">
              ${this.escapeHtml(category.description)}
            </div>
          </div>
          <div class="ml-2 flex items-center gap-2">
            <span class="text-xs bg-gray-100 dark:bg-gray-700 px-2 py-1 rounded">
              ${category.queries.length}
            </span>
            <span class="text-gray-500">${isExpanded ? '▼' : '▶'}</span>
          </div>
        </button>

        ${isExpanded ? `
          <div id="category-${this.escapeHtml(category.id)}" class="bg-gray-50 dark:bg-gray-900/50">
            ${category.queries.map(query => this.renderQuery(query)).join('')}
          </div>
        ` : ''}
      </div>
    `
  }

  private renderQuery(query: QueryExample): string {
    return `
      <button
        class="example-item w-full px-4 py-3 text-left border-t border-gray-200 dark:border-gray-700 hover:bg-white dark:hover:bg-gray-800 transition-colors"
        data-example-id="${this.escapeHtml(query.id)}"
        aria-label="Load ${this.escapeHtml(query.title)} example"
      >
        <div class="flex items-start justify-between gap-2">
          <div class="flex-1 min-w-0">
            <div class="font-medium text-sm text-gray-900 dark:text-gray-100">
              ${this.escapeHtml(query.title)}
            </div>
            <div class="text-xs text-gray-600 dark:text-gray-400 mt-1">
              ${this.escapeHtml(query.description)}
            </div>
            ${query.sqlFeatures.length > 0 ? `
              <div class="flex flex-wrap gap-1 mt-2">
                ${query.sqlFeatures.slice(0, 3).map(feature => `
                  <span class="text-xs bg-blue-100 dark:bg-blue-900 text-blue-800 dark:text-blue-200 px-2 py-0.5 rounded">
                    ${this.escapeHtml(feature)}
                  </span>
                `).join('')}
                ${query.sqlFeatures.length > 3 ? `
                  <span class="text-xs text-gray-500 dark:text-gray-400">
                    +${query.sqlFeatures.length - 3} more
                  </span>
                ` : ''}
              </div>
            ` : ''}
          </div>
          <div class="flex-shrink-0">
            <span class="text-xs px-2 py-1 rounded ${this.getDatabaseBadgeClass(query.database)}">
              ${this.getDatabaseLabel(query.database)}
            </span>
          </div>
        </div>
      </button>
    `
  }

  private getDatabaseLabel(database: string): string {
    switch (database) {
      case 'northwind':
        return 'Northwind'
      case 'employees':
        return 'Employees'
      case 'empty':
        return 'Empty'
      default:
        return database
    }
  }

  private getDatabaseBadgeClass(database: string): string {
    switch (database) {
      case 'northwind':
        return 'bg-green-100 dark:bg-green-900 text-green-800 dark:text-green-200'
      case 'employees':
        return 'bg-purple-100 dark:bg-purple-900 text-purple-800 dark:text-purple-200'
      case 'empty':
        return 'bg-gray-100 dark:bg-gray-700 text-gray-800 dark:text-gray-200'
      default:
        return 'bg-gray-100 dark:bg-gray-700 text-gray-800 dark:text-gray-200'
    }
  }

  private setupEventListeners(): void {
    // Category toggle listeners
    const categoryToggles = this.element.querySelectorAll('.category-toggle')
    categoryToggles.forEach(toggle => {
      toggle.addEventListener('click', () => {
        const categoryId = toggle.getAttribute('data-category-id')
        if (categoryId) {
          this.toggleCategory(categoryId)
        }
      })
    })

    // Example selection listeners
    const exampleItems = this.element.querySelectorAll('.example-item')
    exampleItems.forEach(item => {
      item.addEventListener('click', () => {
        const exampleId = item.getAttribute('data-example-id')
        if (exampleId) {
          const example = this.findExampleById(exampleId)
          if (example) {
            this.selectExample(example)
          }
        }
      })
    })
  }

  private findExampleById(id: string): QueryExample | undefined {
    for (const category of exampleCategories) {
      const example = category.queries.find(q => q.id === id)
      if (example) {
        return example
      }
    }
    return undefined
  }
}

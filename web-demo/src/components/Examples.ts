import { Component } from './base'
import { exampleCategories, type QueryExample } from '../data/examples'

interface ExamplesState {
  expandedCategories: Set<string>
  searchQuery: string
  difficultyFilter: string // 'all' | 'beginner' | 'intermediate' | 'advanced'
  useCaseFilter: string // 'all' | 'analytics' | 'admin' | 'development' | 'reports' | 'data-quality'
}

export interface ExampleSelectEvent {
  sql: string
  database: 'northwind' | 'employees' | 'company' | 'university' | 'empty' | 'sqllogictest'
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
      searchQuery: '',
      difficultyFilter: 'all',
      useCaseFilter: 'all',
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

  /**
   * Update search query
   */
  setSearchQuery(query: string): void {
    this.setState({ searchQuery: query.toLowerCase() })
  }

  /**
   * Update difficulty filter
   */
  setDifficultyFilter(difficulty: string): void {
    this.setState({ difficultyFilter: difficulty })
  }

  /**
   * Update use case filter
   */
  setUseCaseFilter(useCase: string): void {
    this.setState({ useCaseFilter: useCase })
  }

  /**
   * Filter examples based on current filters
   */
  private matchesFilters(example: QueryExample): boolean {
    const { searchQuery, difficultyFilter, useCaseFilter } = this.state

    // Search filter
    if (searchQuery) {
      const searchLower = searchQuery
      const matchesTitle = example.title.toLowerCase().includes(searchLower)
      const matchesDescription = example.description.toLowerCase().includes(searchLower)
      const matchesFeatures = example.sqlFeatures.some(f => f.toLowerCase().includes(searchLower))
      const matchesTags = example.tags?.some(t => t.toLowerCase().includes(searchLower)) || false

      if (!matchesTitle && !matchesDescription && !matchesFeatures && !matchesTags) {
        return false
      }
    }

    // Difficulty filter
    if (difficultyFilter !== 'all' && example.difficulty !== difficultyFilter) {
      return false
    }

    // Use case filter
    if (useCaseFilter !== 'all' && example.useCase !== useCaseFilter) {
      return false
    }

    return true
  }

  /**
   * Get filtered examples for a category
   */
  private getFilteredQueries(queries: QueryExample[]): QueryExample[] {
    return queries.filter(q => this.matchesFilters(q))
  }

  protected render(): void {
    const { expandedCategories, searchQuery, difficultyFilter, useCaseFilter } = this.state

    // Get filtered count
    const filteredCount = exampleCategories.reduce((sum, cat) => {
      return sum + this.getFilteredQueries(cat.queries).length
    }, 0)

    this.element.innerHTML = `
      <div class="bg-background rounded-lg shadow-lg border border-border">
        <div class="px-4 py-3 border-b border-border">
          <h2 class="font-semibold text-foreground">
            SQL:1999 Examples
          </h2>
          <p class="text-xs text-muted mt-1">
            ${filteredCount} of ${this.getTotalExampleCount()} queries across ${exampleCategories.length} categories
          </p>
        </div>

        <!-- Filters -->
        <div class="px-4 py-3 border-b border-border bg-card/30 space-y-3">
          <!-- Search -->
          <div>
            <input
              type="text"
              id="example-search"
              placeholder="Search examples..."
              value="${this.escapeHtml(searchQuery)}"
              class="w-full px-3 py-2 text-sm border border-border rounded bg-background text-foreground placeholder-muted focus:outline-none focus:ring-2 focus:ring-blue-500"
            />
          </div>

          <!-- Filters Row -->
          <div class="flex flex-wrap gap-2">
            <!-- Difficulty Filter -->
            <div class="flex-1 min-w-[150px]">
              <select
                id="difficulty-filter"
                class="w-full px-3 py-2 text-sm border border-border rounded bg-background text-foreground focus:outline-none focus:ring-2 focus:ring-blue-500"
              >
                <option value="all" ${difficultyFilter === 'all' ? 'selected' : ''}>All Difficulties</option>
                <option value="beginner" ${difficultyFilter === 'beginner' ? 'selected' : ''}>Beginner</option>
                <option value="intermediate" ${difficultyFilter === 'intermediate' ? 'selected' : ''}>Intermediate</option>
                <option value="advanced" ${difficultyFilter === 'advanced' ? 'selected' : ''}>Advanced</option>
              </select>
            </div>

            <!-- Use Case Filter -->
            <div class="flex-1 min-w-[150px]">
              <select
                id="usecase-filter"
                class="w-full px-3 py-2 text-sm border border-border rounded bg-background text-foreground focus:outline-none focus:ring-2 focus:ring-blue-500"
              >
                <option value="all" ${useCaseFilter === 'all' ? 'selected' : ''}>All Use Cases</option>
                <option value="analytics" ${useCaseFilter === 'analytics' ? 'selected' : ''}>Analytics</option>
                <option value="reports" ${useCaseFilter === 'reports' ? 'selected' : ''}>Reports</option>
                <option value="development" ${useCaseFilter === 'development' ? 'selected' : ''}>Development</option>
                <option value="data-quality" ${useCaseFilter === 'data-quality' ? 'selected' : ''}>Data Quality</option>
                <option value="admin" ${useCaseFilter === 'admin' ? 'selected' : ''}>Admin</option>
              </select>
            </div>

            <!-- Clear Filters -->
            ${searchQuery || difficultyFilter !== 'all' || useCaseFilter !== 'all' ? `
              <button
                id="clear-filters"
                class="px-3 py-2 text-sm bg-gray-100 dark:bg-gray-700 hover:bg-gray-200 dark:hover:bg-gray-600 text-gray-700 dark:text-gray-300 rounded transition-colors"
              >
                Clear Filters
              </button>
            ` : ''}
          </div>
        </div>

        <div class="divide-y divide-border overflow-y-auto max-h-[60vh]">
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
    const filteredQueries = this.getFilteredQueries(category.queries)

    // Hide category if no queries match filters
    if (filteredQueries.length === 0) {
      return ''
    }

    return `
      <div class="category-section">
        <button
          class="category-toggle w-full px-4 py-3 text-left hover:bg-card/50 transition-colors flex justify-between items-center"
          data-category-id="${this.escapeHtml(category.id)}"
          aria-expanded="${isExpanded}"
          aria-controls="category-${this.escapeHtml(category.id)}"
        >
          <div class="flex-1">
            <div class="font-medium text-sm text-foreground">
              ${this.escapeHtml(category.title)}
            </div>
            <div class="text-xs text-muted mt-1">
              ${this.escapeHtml(category.description)}
            </div>
          </div>
          <div class="ml-2 flex items-center gap-2">
            <span class="text-xs bg-card px-2 py-1 rounded">
              ${filteredQueries.length}${filteredQueries.length !== category.queries.length ? `/${category.queries.length}` : ''}
            </span>
            <span class="text-muted">${isExpanded ? '▼' : '▶'}</span>
          </div>
        </button>

        ${isExpanded ? `
          <div id="category-${this.escapeHtml(category.id)}" class="bg-card/30">
            ${filteredQueries.map(query => this.renderQuery(query)).join('')}
          </div>
        ` : ''}
      </div>
    `
  }

  private renderQuery(query: QueryExample): string {
    return `
      <button
        class="example-item w-full px-4 py-3 text-left border-t border-border hover:bg-background transition-colors"
        data-example-id="${this.escapeHtml(query.id)}"
        aria-label="Load ${this.escapeHtml(query.title)} example"
      >
        <div class="flex items-start justify-between gap-2">
          <div class="flex-1 min-w-0">
            <div class="flex items-center gap-2 flex-wrap">
              <div class="font-medium text-sm text-foreground">
                ${this.escapeHtml(query.title)}
              </div>
              ${query.difficulty ? `
                <span class="text-xs px-2 py-0.5 rounded ${this.getDifficultyBadgeClass(query.difficulty)}">
                  ${query.difficulty}
                </span>
              ` : ''}
              ${query.useCase ? `
                <span class="text-xs px-2 py-0.5 rounded ${this.getUseCaseBadgeClass(query.useCase)}">
                  ${this.formatUseCase(query.useCase)}
                </span>
              ` : ''}
            </div>
            <div class="text-xs text-muted mt-1">
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
                  <span class="text-xs text-muted">
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

  private getDifficultyBadgeClass(difficulty: string): string {
    switch (difficulty) {
      case 'beginner':
        return 'bg-green-100 dark:bg-green-900 text-green-800 dark:text-green-200'
      case 'intermediate':
        return 'bg-yellow-100 dark:bg-yellow-900 text-yellow-800 dark:text-yellow-200'
      case 'advanced':
        return 'bg-red-100 dark:bg-red-900 text-red-800 dark:text-red-200'
      default:
        return 'bg-gray-100 dark:bg-gray-700 text-gray-800 dark:text-gray-200'
    }
  }

  private getUseCaseBadgeClass(useCase: string): string {
    switch (useCase) {
      case 'analytics':
        return 'bg-purple-100 dark:bg-purple-900 text-purple-800 dark:text-purple-200'
      case 'reports':
        return 'bg-blue-100 dark:bg-blue-900 text-blue-800 dark:text-blue-200'
      case 'development':
        return 'bg-indigo-100 dark:bg-indigo-900 text-indigo-800 dark:text-indigo-200'
      case 'data-quality':
        return 'bg-orange-100 dark:bg-orange-900 text-orange-800 dark:text-orange-200'
      case 'admin':
        return 'bg-pink-100 dark:bg-pink-900 text-pink-800 dark:text-pink-200'
      default:
        return 'bg-gray-100 dark:bg-gray-700 text-gray-800 dark:text-gray-200'
    }
  }

  private formatUseCase(useCase: string): string {
    return useCase.split('-').map(word => word.charAt(0).toUpperCase() + word.slice(1)).join(' ')
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
    // Search input
    const searchInput = document.getElementById('example-search') as HTMLInputElement
    if (searchInput) {
      searchInput.addEventListener('input', (e) => {
        const target = e.target as HTMLInputElement
        this.setSearchQuery(target.value)
      })
    }

    // Difficulty filter
    const difficultyFilter = document.getElementById('difficulty-filter') as HTMLSelectElement
    if (difficultyFilter) {
      difficultyFilter.addEventListener('change', (e) => {
        const target = e.target as HTMLSelectElement
        this.setDifficultyFilter(target.value)
      })
    }

    // Use case filter
    const useCaseFilter = document.getElementById('usecase-filter') as HTMLSelectElement
    if (useCaseFilter) {
      useCaseFilter.addEventListener('change', (e) => {
        const target = e.target as HTMLSelectElement
        this.setUseCaseFilter(target.value)
      })
    }

    // Clear filters button
    const clearButton = document.getElementById('clear-filters')
    if (clearButton) {
      clearButton.addEventListener('click', () => {
        this.setState({
          searchQuery: '',
          difficultyFilter: 'all',
          useCaseFilter: 'all',
        })
      })
    }

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
      const example = category.queries.find((q: QueryExample) => q.id === id)
      if (example) {
        return example
      }
    }
    return undefined
  }
}

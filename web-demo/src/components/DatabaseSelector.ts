import { Component } from './base'

export interface DatabaseOption {
  id: string
  name: string
  description: string
}

interface DatabaseSelectorState {
  databases: DatabaseOption[]
  selected: string
}

/**
 * Database selector component - switch between different example databases
 */
export class DatabaseSelectorComponent extends Component<DatabaseSelectorState> {
  private onChangeCallback: ((dbId: string) => void) | null = null

  constructor(databases: DatabaseOption[], selectedId: string = databases[0]?.id || '') {
    super('#database-selector', {
      databases,
      selected: selectedId,
    })
  }

  /**
   * Register callback when database selection changes
   */
  onChange(callback: (dbId: string) => void): void {
    this.onChangeCallback = callback
  }

  /**
   * Get currently selected database ID
   */
  getSelected(): string {
    return this.state.selected
  }

  /**
   * Set selected database
   */
  setSelected(dbId: string): void {
    if (this.state.databases.some(db => db.id === dbId)) {
      this.setState({ selected: dbId })
      if (this.onChangeCallback) {
        this.onChangeCallback(dbId)
      }
    }
  }

  protected render(): void {
    const { databases, selected } = this.state

    if (databases.length === 0) {
      this.element.innerHTML = '<p class="text-muted">No databases available</p>'
      return
    }

    const selectedDb = databases.find(db => db.id === selected)

    this.element.innerHTML = `
      <div class="inline-block relative">
        <label for="db-select" class="sr-only">Select Database</label>
        <select
          id="db-select"
          class="appearance-none bg-background border border-border rounded-lg px-4 py-2 pr-8 text-sm text-foreground focus:outline-none focus:ring-2 focus:ring-primary-light dark:focus:ring-primary-dark cursor-pointer"
          aria-label="Select database"
        >
          ${databases.map(db => `
            <option value="${this.escapeHtml(db.id)}" ${db.id === selected ? 'selected' : ''}>
              ${this.escapeHtml(db.name)}
            </option>
          `).join('')}
        </select>
        <div class="pointer-events-none absolute inset-y-0 right-0 flex items-center px-2 text-muted">
          <svg class="fill-current h-4 w-4" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20">
            <path d="M5.293 7.293a1 1 0 011.414 0L10 10.586l3.293-3.293a1 1 0 111.414 1.414l-4 4a1 1 0 01-1.414 0l-4-4a1 1 0 010-1.414z"/>
          </svg>
        </div>
      </div>
      ${selectedDb ? `
        <p class="text-xs text-muted mt-1">
          ${this.escapeHtml(selectedDb.description)}
        </p>
      ` : ''}
    `

    // Setup change handler
    const select = this.element.querySelector('#db-select') as HTMLSelectElement
    if (select) {
      select.addEventListener('change', (e) => {
        const newValue = (e.target as HTMLSelectElement).value
        this.setSelected(newValue)
      })
    }
  }
}

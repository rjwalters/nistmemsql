import { ShowcaseNav } from '../components/ShowcaseNav'
import { DataTypesShowcase } from '../components/DataTypesShowcase'
import { SubqueriesShowcase } from '../components/SubqueriesShowcase'
import { PredicatesShowcase } from '../components/PredicatesShowcase'
import { AggregatesShowcase } from '../components/AggregatesShowcase'
import { DMLOperationsShowcase } from '../components/DMLOperationsShowcase'

type ShowcaseComponent =
  | DataTypesShowcase
  | SubqueriesShowcase
  | PredicatesShowcase
  | AggregatesShowcase
  | DMLOperationsShowcase

export class ShowcaseController {
  private nav: ShowcaseNav
  private container: HTMLElement
  private contentContainer: HTMLElement
  private isVisible: boolean = false
  private currentShowcase: ShowcaseComponent | null = null

  constructor(containerId: string) {
    const container = document.getElementById(containerId)
    if (!container) {
      throw new Error(`Showcase container '${containerId}' not found`)
    }

    this.container = container
    this.nav = new ShowcaseNav()

    // Create content container for showcase components
    this.contentContainer = document.createElement('div')
    this.contentContainer.id = 'showcase-content'
    this.contentContainer.className = 'mt-6'

    this.setupListeners()
  }

  private setupListeners(): void {
    // Listen for category selection
    this.container.addEventListener('category-selected', ((e: CustomEvent) => {
      const { categoryId } = e.detail
      this.loadCategory(categoryId)
    }) as EventListener)

    // Listen for example loading from showcases
    this.container.addEventListener('load-example', ((e: CustomEvent) => {
      const { query } = e.detail
      this.loadExampleIntoEditor(query)
    }) as EventListener)
  }

  private loadCategory(categoryId: string): void {
    // Clear current showcase
    if (this.currentShowcase) {
      this.currentShowcase.unmount()
      this.currentShowcase = null
    }

    // Load appropriate showcase based on category
    switch (categoryId) {
      case 'data-types':
        this.currentShowcase = new DataTypesShowcase()
        this.currentShowcase.mount(this.contentContainer)
        break

      case 'subqueries':
        this.currentShowcase = new SubqueriesShowcase()
        this.currentShowcase.mount(this.contentContainer)
        break

      case 'predicates':
        this.currentShowcase = new PredicatesShowcase()
        this.currentShowcase.mount(this.contentContainer)
        break

      case 'aggregates':
        this.currentShowcase = new AggregatesShowcase()
        this.currentShowcase.mount(this.contentContainer)
        break

      case 'dml':
        this.currentShowcase = new DMLOperationsShowcase()
        this.currentShowcase.mount(this.contentContainer)
        break

      // Future categories will be added here:
      // case 'joins': ...
      // etc.

      default:
        // Display placeholder for unimplemented categories
        this.contentContainer.innerHTML = `
          <div class="bg-yellow-50 dark:bg-yellow-900/20 border border-yellow-200 dark:border-yellow-800 rounded-lg p-6 text-center">
            <p class="text-yellow-800 dark:text-yellow-200 font-medium mb-2">
              Coming Soon
            </p>
            <p class="text-yellow-700 dark:text-yellow-300 text-sm">
              The ${categoryId} showcase is currently being developed.
            </p>
          </div>
        `
    }
  }

  private loadExampleIntoEditor(query: string): void {
    // Find Monaco editor and load the example query
    const editorElement = document.querySelector('.monaco-editor')
    if (
      editorElement &&
      (
        window as {
          monaco?: { editor?: { getEditors?: () => { setValue: (v: string) => void }[] } }
        }
      ).monaco
    ) {
      const editors = (
        window as { monaco: { editor: { getEditors: () => { setValue: (v: string) => void }[] } } }
      ).monaco.editor.getEditors()
      if (editors && editors.length > 0) {
        editors[0].setValue(query)
      }
    }
  }

  public toggle(): void {
    if (this.isVisible) {
      this.hide()
    } else {
      this.show()
    }
  }

  public show(): void {
    if (!this.isVisible) {
      this.container.classList.remove('hidden')
      this.nav.mount(this.container)
      this.container.appendChild(this.contentContainer)
      this.isVisible = true
    }
  }

  public hide(): void {
    if (this.isVisible) {
      this.container.classList.add('hidden')
      this.nav.unmount()
      if (this.currentShowcase) {
        this.currentShowcase.unmount()
        this.currentShowcase = null
      }
      this.isVisible = false
    }
  }

  public isShowing(): boolean {
    return this.isVisible
  }
}

export function initShowcase(): ShowcaseController {
  const controller = new ShowcaseController('showcase-nav')

  // Set up toggle button
  const toggleButton = document.getElementById('showcase-toggle')
  if (toggleButton) {
    toggleButton.addEventListener('click', () => {
      controller.toggle()

      // Update button styling based on state
      if (controller.isShowing()) {
        toggleButton.classList.add('text-blue-600', 'dark:text-blue-400', 'font-semibold')
      } else {
        toggleButton.classList.remove('text-blue-600', 'dark:text-blue-400', 'font-semibold')
      }
    })
  }

  return controller
}

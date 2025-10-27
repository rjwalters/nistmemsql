import { ShowcaseNav } from '../components/ShowcaseNav'

export class ShowcaseController {
  private nav: ShowcaseNav
  private container: HTMLElement
  private isVisible: boolean = false

  constructor(containerId: string) {
    const container = document.getElementById(containerId)
    if (!container) {
      throw new Error(`Showcase container '${containerId}' not found`)
    }

    this.container = container
    this.nav = new ShowcaseNav()
    this.setupListeners()
  }

  private setupListeners(): void {
    // Listen for category selection
    this.container.addEventListener('category-selected', ((e: CustomEvent) => {
      const { categoryId } = e.detail
      // Future: Load category-specific content
      this.loadCategory(categoryId)
    }) as EventListener)
  }

  private loadCategory(categoryId: string): void {
    // Placeholder for loading category-specific examples and content
    // This will be implemented in subsequent issues (#146, #147)
    // Category content loading will go here
    void categoryId // Suppress unused variable warning
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
      this.isVisible = true
    }
  }

  public hide(): void {
    if (this.isVisible) {
      this.container.classList.add('hidden')
      this.nav.unmount()
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

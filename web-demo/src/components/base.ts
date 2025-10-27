/**
 * Base component class with state management and rendering
 *
 * Provides a reactive pattern where setState() triggers re-rendering.
 * All components extend this class and implement the render() method.
 */
export abstract class Component<TState = Record<string, unknown>> {
  protected element: HTMLElement
  protected state: TState

  constructor(selector: string, initialState: TState) {
    const el = document.querySelector(selector)
    if (!el) {
      throw new Error(`Element not found: ${selector}`)
    }

    this.element = el as HTMLElement
    this.state = initialState
    this.render()
  }

  /**
   * Update component state and trigger re-render
   */
  protected setState(newState: Partial<TState>): void {
    this.state = { ...this.state, ...newState }
    this.render()
  }

  /**
   * Render the component - must be implemented by subclasses
   */
  protected abstract render(): void

  /**
   * Escape HTML to prevent XSS attacks
   */
  protected escapeHtml(str: string): string {
    const div = document.createElement('div')
    div.textContent = str
    return div.innerHTML
  }
}

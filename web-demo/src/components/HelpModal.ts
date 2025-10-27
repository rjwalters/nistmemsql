import { Component } from './base'

interface HelpModalState {
  isOpen: boolean
}

/**
 * Help modal showing keyboard shortcuts and usage tips
 */
export class HelpModal extends Component<HelpModalState> {
  constructor() {
    super('#help-modal', { isOpen: false })
    this.attachGlobalHandlers()
  }

  open(): void {
    this.setState({ isOpen: true })
    document.body.style.overflow = 'hidden' // Prevent background scrolling
  }

  close(): void {
    this.setState({ isOpen: false })
    document.body.style.overflow = '' // Restore scrolling
  }

  toggle(): void {
    if (this.state.isOpen) {
      this.close()
    } else {
      this.open()
    }
  }

  private attachGlobalHandlers(): void {
    // Open help with ? or F1
    document.addEventListener('keydown', (e) => {
      if (e.key === '?' && !this.isInputFocused()) {
        e.preventDefault()
        this.open()
      } else if (e.key === 'F1') {
        e.preventDefault()
        this.open()
      } else if (e.key === 'Escape' && this.state.isOpen) {
        e.preventDefault()
        this.close()
      }
    })
  }

  private isInputFocused(): boolean {
    const active = document.activeElement
    return (
      active?.tagName === 'INPUT' ||
      active?.tagName === 'TEXTAREA' ||
      (active?.hasAttribute('contenteditable') ?? false)
    )
  }

  protected render(): void {
    if (!this.state.isOpen) {
      this.element.innerHTML = ''
      return
    }

    this.element.innerHTML = this.renderModal()
    this.attachModalHandlers()
  }

  private renderModal(): string {
    return `
      <div class="modal-overlay" data-action="close-overlay">
        <div class="modal-content">
          <div class="modal-header">
            <h2 class="text-2xl font-bold text-gray-900 dark:text-gray-100">
              Keyboard Shortcuts & Help
            </h2>
            <button class="modal-close" data-action="close-modal" type="button" aria-label="Close">
              ×
            </button>
          </div>

          <div class="modal-body">
            <section class="help-section">
              <h3 class="help-section__title">Editor Shortcuts</h3>
              <dl class="help-shortcuts">
                <div class="help-shortcut">
                  <dt><kbd>⌘/Ctrl</kbd> + <kbd>Enter</kbd></dt>
                  <dd>Execute current query</dd>
                </div>
                <div class="help-shortcut">
                  <dt><kbd>⌘/Ctrl</kbd> + <kbd>/</kbd></dt>
                  <dd>Toggle line comment</dd>
                </div>
                <div class="help-shortcut">
                  <dt><kbd>Tab</kbd></dt>
                  <dd>Indent selection</dd>
                </div>
              </dl>
            </section>

            <section class="help-section">
              <h3 class="help-section__title">Navigation</h3>
              <dl class="help-shortcuts">
                <div class="help-shortcut">
                  <dt><kbd>?</kbd> or <kbd>F1</kbd></dt>
                  <dd>Show this help dialog</dd>
                </div>
                <div class="help-shortcut">
                  <dt><kbd>Esc</kbd></dt>
                  <dd>Close help dialog</dd>
                </div>
              </dl>
            </section>

            <section class="help-section">
              <h3 class="help-section__title">Results Actions</h3>
              <dl class="help-shortcuts">
                <div class="help-shortcut">
                  <dt>Copy to clipboard</dt>
                  <dd>Copy results as tab-separated values</dd>
                </div>
                <div class="help-shortcut">
                  <dt>Export CSV</dt>
                  <dd>Download results as CSV file</dd>
                </div>
              </dl>
            </section>

            <section class="help-section">
              <h3 class="help-section__title">Tips</h3>
              <ul class="help-tips">
                <li>Results are limited to 1,000 rows for performance. Use <code>LIMIT</code> clause to refine queries.</li>
                <li>Execution time is shown with query results.</li>
                <li>The editor supports SQL syntax highlighting and auto-completion.</li>
                <li>Toggle between light/dark themes using the theme button.</li>
              </ul>
            </section>
          </div>

          <div class="modal-footer">
            <button class="button" data-action="close-modal" type="button">
              Got it!
            </button>
          </div>
        </div>
      </div>
    `
  }

  private attachModalHandlers(): void {
    const closeButtons = this.element.querySelectorAll<HTMLElement>('[data-action="close-modal"]')
    const overlay = this.element.querySelector<HTMLElement>('[data-action="close-overlay"]')

    closeButtons.forEach((button) => {
      button.addEventListener('click', () => this.close())
    })

    overlay?.addEventListener('click', (e) => {
      if (e.target === overlay) {
        this.close()
      }
    })
  }
}

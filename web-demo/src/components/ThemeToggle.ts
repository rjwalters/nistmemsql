import { Component } from './base'
import { Theme } from '../theme'

interface ThemeToggleState {
  theme: 'light' | 'dark'
}

/**
 * Theme toggle component - switches between light and dark mode
 */
export class ThemeToggleComponent extends Component<ThemeToggleState> {
  private themeSystem: Theme

  constructor(themeSystem: Theme) {
    const currentTheme = themeSystem.current as 'light' | 'dark'
    super('#theme-toggle', { theme: currentTheme })
    this.themeSystem = themeSystem
  }

  /**
   * Toggle between light and dark mode
   */
  toggle(): void {
    const isDark = this.themeSystem.toggle()
    this.setState({ theme: isDark ? 'dark' : 'light' })
  }

  protected render(): void {
    const { theme } = this.state
    const icon = theme === 'dark' ? '☀️' : '🌙'
    const label = theme === 'dark' ? 'Light' : 'Dark'

    this.element.innerHTML = `
      <button
        id="theme-toggle-btn"
        class="btn-primary flex items-center gap-2"
        aria-label="Toggle ${label} mode"
      >
        <span>${icon}</span>
        <span>${label} Mode</span>
      </button>
    `

    // Setup click handler
    const btn = this.element.querySelector('#theme-toggle-btn')
    if (btn) {
      btn.addEventListener('click', () => this.toggle())
    }
  }
}

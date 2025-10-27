/**
 * Theme management system for dark mode support
 *
 * Implements class-based dark mode with:
 * - System preference detection
 * - localStorage persistence
 * - Manual toggle capability
 */

export interface Theme {
  /** Toggle between light and dark mode */
  toggle(): boolean
  /** Get current theme */
  readonly current: 'light' | 'dark'
}

/**
 * Initialize the theme system.
 *
 * On first load:
 * 1. Check localStorage for saved preference
 * 2. Fall back to system preference if no saved preference
 * 3. Apply theme to document root
 *
 * @returns Theme controller with toggle() and current property
 */
export function initTheme(): Theme {
  // Check localStorage first, then system preference
  const savedTheme = localStorage.getItem('theme')
  const prefersDark = window.matchMedia('(prefers-color-scheme: dark)').matches
  const theme = savedTheme ?? (prefersDark ? 'dark' : 'light')

  // Apply initial theme
  document.documentElement.classList.toggle('dark', theme === 'dark')

  return {
    toggle() {
      const isDark = document.documentElement.classList.toggle('dark')
      localStorage.setItem('theme', isDark ? 'dark' : 'light')
      return isDark
    },

    get current() {
      return document.documentElement.classList.contains('dark') ? 'dark' : 'light'
    }
  }
}

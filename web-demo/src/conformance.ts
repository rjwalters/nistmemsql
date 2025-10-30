import './styles/main.css'
import { initTheme } from './theme'
import { ThemeToggleComponent } from './components/ThemeToggle'
import { ConformanceReportComponent } from './components/ConformanceReport'

/**
 * Bootstrap the conformance report page
 */
function bootstrap(): void {
  // Initialize theme system (with localStorage persistence)
  const theme = initTheme()

  // Initialize theme toggle component
  const themeToggleContainer = document.querySelector<HTMLDivElement>('#theme-toggle')
  if (themeToggleContainer) {
    new ThemeToggleComponent(theme)
  }

  // Initialize conformance report component
  new ConformanceReportComponent()
}

// Start the application
bootstrap()

import './styles/main.css'
import { initTheme } from './theme'
import { NavigationComponent } from './components/Navigation'
import { ConformanceReportComponent } from './components/ConformanceReport'

/**
 * Bootstrap the conformance report page
 */
function bootstrap(): void {
  // Initialize theme system (with localStorage persistence)
  const theme = initTheme()

  // Initialize navigation component
  new NavigationComponent('conformance', theme)

  // Initialize conformance report component
  new ConformanceReportComponent()
}

// Start the application
bootstrap()

import './styles/main.css'
import { initTheme } from './theme'
import { NavigationComponent } from './components/Navigation'
import { ConformanceReportComponent } from './components/ConformanceReport'
import { updateConformanceFooter } from './utils/conformance'

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

  // Update conformance pass rate dynamically in footer
  void updateConformanceFooter()
}

// Start the application when DOM is ready
document.addEventListener('DOMContentLoaded', bootstrap)

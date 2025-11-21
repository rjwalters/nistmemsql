import './styles/main.css'
import { getStorageMode, isOpfsSupported } from './db/wasm'
import { NavigationComponent } from './components/Navigation'
import { ExamplesComponent } from './components/Examples'
import type { ExampleSelectEvent } from './components/Examples'
import { DatabaseSelectorComponent } from './components/DatabaseSelector'
import type { DatabaseOption } from './components/DatabaseSelector'
import { LoadingProgressComponent } from './components/LoadingProgress'
import { initShowcase } from './showcase'
import { sampleDatabases } from './data/sample-databases'
import { updateConformanceFooter } from './utils/conformance'
import { initializeApp, setupThemeSync } from './app/initialization'
import { createExecutionHandler } from './app/query-executor'

async function bootstrap(): Promise<void> {
  // Initialize loading progress indicator
  const progress = new LoadingProgressComponent()
  progress.addStep('theme', 'Initializing theme')
  progress.addStep('editor', 'Preparing editor')
  progress.addStep('wasm', 'Loading database engine')
  progress.addStep('ui', 'Setting up user interface')

  // Hide loading indicator when complete
  progress.onComplete(() => {
    setTimeout(() => progress.hide(), 500)
  })

  try {
    // Initialize core application components
    const app = await initializeApp(progress)

    // Setup theme synchronization with editor
    setupThemeSync(app.theme, mode => app.editor.applyTheme(mode))

    // Initialize Navigation component
    progress.updateStep('ui', 70, 'loading')
    new NavigationComponent('terminal', app.theme)

    // Update storage status display
    const storageStatusEl = document.getElementById('storage-status')
    if (storageStatusEl) {
      const storageMode = getStorageMode()
      storageStatusEl.textContent = storageMode

      // Add visual indicator for OPFS vs in-memory
      if (isOpfsSupported() && storageMode.includes('OPFS')) {
        storageStatusEl.classList.add('text-green-600', 'dark:text-green-400')
        storageStatusEl.title =
          'Data persists across browser sessions using Origin Private File System'
      } else {
        storageStatusEl.classList.add('text-yellow-600', 'dark:text-yellow-400')
        storageStatusEl.title = 'Data is temporary and will be lost when the page reloads'
      }
    }

    // Pre-load default sample database for immediate exploration
    await app.database.loadDatabase('employees')

    // Create execution handler (temporary, will be recreated after Monaco upgrade)
    let executeHandler = createExecutionHandler(
      app.editor.getEditor(),
      app.database.getDatabase(),
      app.editor.getResultsEditor(),
      () => app.database.refreshTables()
    )

    // Function to upgrade editors to Monaco and recreate handler
    const upgradeEditorsToMonaco = async (): Promise<void> => {
      await app.editor.upgradeToMonaco(
        () => app.database.getTableNames(),
        () => executeHandler()
      )

      // Recreate execute handler after upgrade
      executeHandler = createExecutionHandler(
        app.editor.getEditor(),
        app.database.getDatabase(),
        app.editor.getResultsEditor(),
        () => app.database.refreshTables()
      )
    }

    // Upgrade to Monaco now (Monaco was preloaded during WASM load)
    // This ensures Monaco is fully rendered before hiding the loader
    progress.updateStep('ui', 80, 'loading')
    console.log('[Bootstrap] Starting Monaco upgrade...')
    await upgradeEditorsToMonaco()
    console.log('[Bootstrap] Monaco upgrade complete')

    // Initialize Database Selector with all available sample databases
    console.log('[Bootstrap] Initializing database selector...')
    const databases: DatabaseOption[] = sampleDatabases.map(db => ({
      id: db.id,
      name: db.name,
      description: db.description,
    }))
    const databaseSelector = new DatabaseSelectorComponent(
      databases,
      app.database.getCurrentDatabaseId()
    )
    databaseSelector.onChange((dbId: string) => {
      void app.database.loadDatabase(dbId)
    })
    console.log('[Bootstrap] Database selector ready')

    // Initialize Examples sidebar
    console.log('[Bootstrap] Initializing examples...')
    const examplesComponent = new ExamplesComponent()
    examplesComponent.onSelect((event: ExampleSelectEvent) => {
      app.editor.getEditor().setValue(event.sql)
      // Switch database if needed
      if (event.database !== app.database.getCurrentDatabaseId()) {
        void app.database.loadDatabase(event.database)
        databaseSelector.setSelected(event.database)
      }
    })
    console.log('[Bootstrap] Examples ready')

    // Run button executes query
    app.layout.runButton?.addEventListener('click', () => {
      void executeHandler()
    })

    // Initialize SQL:1999 Showcase navigation
    console.log('[Bootstrap] Initializing showcase...')
    initShowcase()
    console.log('[Bootstrap] Showcase ready')

    // Set build timestamp in footer
    console.log('[Bootstrap] Setting build timestamp...')
    const timestampElement = document.getElementById('build-timestamp')
    if (timestampElement) {
      try {
        const timestamp = __BUILD_TIMESTAMP__
        const date = new Date(timestamp)
        const formattedDate = date.toLocaleString('en-US', {
          year: 'numeric',
          month: 'short',
          day: 'numeric',
          hour: '2-digit',
          minute: '2-digit',
          timeZoneName: 'short',
        })
        timestampElement.textContent = `Deployed: ${formattedDate}`
      } catch (error) {
        console.warn('Failed to set build timestamp', error)
        timestampElement.textContent = 'Deployed: Development build'
      }
    }

    // Update conformance pass rate dynamically
    console.log('[Bootstrap] Updating conformance footer...')
    // Don't wait for conformance update, it can complete in background
    updateConformanceFooter()
      .then(() => console.log('[Bootstrap] Conformance footer updated'))
      .catch(err => console.warn('[Bootstrap] Conformance footer failed:', err))

    // Final UI setup complete
    console.log('[Bootstrap] Finalizing UI...')
    progress.updateStep('ui', 95, 'loading')

    // Small delay to show completion state
    await new Promise(resolve => setTimeout(resolve, 200))
    console.log('[Bootstrap] Completing UI step...')
    progress.completeStep('ui')
    console.log('[Bootstrap] Bootstrap complete!')
  } catch (error) {
    console.error('Bootstrap error:', error)
    const message = error instanceof Error ? error.message : String(error)
    progress.errorStep('ui', `Initialization failed: ${message}`)
  }
}

// Start the application when DOM is ready
document.addEventListener('DOMContentLoaded', () => void bootstrap())
